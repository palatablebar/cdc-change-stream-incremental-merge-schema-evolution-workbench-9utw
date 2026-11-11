"""schema 发布：按真实记录预检越界 → 隔离坏记录 → 发布/回退。

核心约定：
- 每个事件按自己的 LSN 选择生效版本（见 schema_engine.resolve_version_for_lsn）；
- 发布前用真实记录（目标表 / CDC 事件 / 上次归并结果）检查新版本是否越界；
- 越界记录单独进拒绝队列（reason=out_of_range），不阻断其他记录；
- 发布失败时版本状态与处理位置（水位线）一起恢复到发布前。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from src.config.settings import settings
from src.constants import error_codes as codes
from src.constants.enums import (
    RejectReason,
    RejectStatus,
    RunStatus,
    SchemaPublishStatus,
    SchemaVersionStatus,
)
from src.models.cdc_event import CdcEvent
from src.models.change_stream import ChangeStream
from src.models.merge_run import MergeRun
from src.models.rejected_event import RejectedEvent
from src.models.schema_publish import SchemaPublish
from src.models.schema_version import SchemaVersion
from src.services import schema_engine
from src.schemas.schema_diff import serialize_schema_version
from src.utils.errors import AppError, not_found
from src.utils.formatters import format_lsn, iso


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _version_payload(stream: ChangeStream, version: SchemaVersion | None) -> dict[str, Any]:
    fields = list(version.fields_json or []) if version else []
    return {
        "version_label": version.version_label if version else "（无）",
        "fields": fields,
        "pk_columns": [f["name"] for f in fields if f.get("is_pk")] or [stream.pk_column],
    }


def _previous_version(versions: list[SchemaVersion], boundary: int) -> SchemaVersion | None:
    """边界之前最后生效的版本；用于生成迁移 SQL 与失败回退目标。"""
    candidates = [
        version
        for version in versions
        if version.effective_from_lsn is not None
        and int(version.effective_from_lsn) < int(boundary)
        and version.status
        not in (SchemaVersionStatus.SUPERSEDED.value, SchemaVersionStatus.ROLLED_BACK.value)
    ]
    candidates.sort(key=lambda item: (int(item.effective_from_lsn), str(item.created_at)))
    if candidates:
        return candidates[-1]
    # 边界落在最早版本之前（例如从 LSN 1 起替换）：以最早生效版本作为迁移起点。
    fallback = [
        version
        for version in versions
        if version.effective_from_lsn is not None
        and version.status
        not in (SchemaVersionStatus.SUPERSEDED.value, SchemaVersionStatus.ROLLED_BACK.value)
    ]
    fallback.sort(key=lambda item: (int(item.effective_from_lsn), str(item.created_at)))
    return fallback[0] if fallback else None


def _target_table(stream: ChangeStream) -> str:
    return f"{settings.target_schema}.{stream.name}_dw"


def _target_records(db: Session, stream: ChangeStream) -> tuple[list[dict[str, Any]], bool]:
    table = f"{stream.name}_dw"
    try:
        inspector = inspect(db.get_bind())
        if not inspector.has_table(table, schema=settings.target_schema):
            return [], False
        rows = db.execute(
            text(f"SELECT * FROM {_q(settings.target_schema)}.{_q(table)}")
        ).mappings().all()
    except Exception:  # noqa: BLE001 - 目标表不可读时退回事件/归并结果
        return [], False
    records = []
    for row in rows:
        values = dict(row)
        pk = values.get(stream.pk_column)
        records.append(
            {
                "source": "target",
                "pk": str(pk) if pk is not None else None,
                "lsn": None,
                "event_id": None,
                "values": values,
            }
        )
    return records, True


def _merge_result_records(db: Session, stream: ChangeStream) -> list[dict[str, Any]]:
    run = (
        db.query(MergeRun)
        .filter(MergeRun.stream_id == stream.id, MergeRun.status == RunStatus.SUCCEEDED.value)
        .order_by(MergeRun.created_at.desc())
        .first()
    )
    if run is None or not run.result_json:
        return []
    records = []
    for row in (run.result_json or {}).get("rows") or []:
        records.append(
            {
                "source": "merge_result",
                "pk": str(row.get("pk")) if row.get("pk") is not None else None,
                "lsn": row.get("last_lsn"),
                "event_id": None,
                "values": dict(row.get("values") or {}),
            }
        )
    return records


def _event_records(db: Session, stream: ChangeStream, boundary: int) -> list[dict[str, Any]]:
    """边界之后的事件才用新版本校验；边界之前的老事件继续用老规则。"""
    rows = (
        db.query(CdcEvent)
        .filter(
            CdcEvent.stream_id == stream.id,
            CdcEvent.lsn >= int(boundary),
            CdcEvent.reject_reason.is_(None),
            CdcEvent.op.in_(("insert", "update")),
            CdcEvent.after_json.isnot(None),
        )
        .order_by(CdcEvent.lsn.asc(), CdcEvent.received_seq.asc())
        .all()
    )
    records = []
    for row in rows:
        values = dict(row.after_json or {})
        pk = row.pk_value if row.pk_value is not None else values.get(stream.pk_column)
        records.append(
            {
                "source": "event",
                "pk": str(pk) if pk is not None else None,
                "lsn": int(row.lsn),
                "event_id": str(row.id),
                "op": row.op,
                "before": row.before_json,
                "after": row.after_json,
                "values": values,
            }
        )
    return records


def collect_real_records(
    db: Session, stream: ChangeStream, boundary: int
) -> tuple[list[dict[str, Any]], list[str]]:
    records: list[dict[str, Any]] = []
    sources: list[str] = []

    target_rows, target_exists = _target_records(db, stream)
    if target_exists:
        sources.append("target")
        records.extend(target_rows)

    event_rows = _event_records(db, stream, boundary)
    if event_rows:
        sources.append("events")
        records.extend(event_rows)

    # 目标表为空时，用最近一次成功归并的落账行兜底（它就是当前目标状态的等价物）
    if not target_rows:
        merged = _merge_result_records(db, stream)
        if merged:
            sources.append("merge_result")
            records.extend(merged)

    return records, sources


def precheck(
    db: Session,
    stream: ChangeStream,
    version: SchemaVersion,
    boundary: int,
) -> dict[str, Any]:
    records, sources = collect_real_records(db, stream, boundary)
    fields = list(version.fields_json or [])
    violations: list[dict[str, Any]] = []
    affected: set[str] = set()

    for record in records:
        # 目标表/归并结果是一整行，按完整记录校验；CDC 事件可能只带变化列，只校验出现过的值。
        require_all = record.get("source") in ("target", "merge_result")
        record_violations = schema_engine.validate_record(
            record.get("values") or {}, fields, require_all=require_all
        )
        for violation in record_violations:
            entry = {
                "pk": record.get("pk"),
                "lsn": record.get("lsn"),
                "source": record.get("source"),
                "event_id": record.get("event_id"),
                **violation,
            }
            violations.append(entry)
            if record.get("pk"):
                affected.add(str(record["pk"]))

    return {
        "checked": len(records),
        "sources": sources,
        "violation_count": len(violations),
        "affected_orders": sorted(affected),
        "affected_count": len(affected),
        "violations": violations,
        "version_label": version.version_label,
        "effective_from_lsn": int(boundary),
        "effective_from_display": format_lsn(int(boundary)),
    }


def _quarantine(
    db: Session,
    stream: ChangeStream,
    violations: list[dict[str, Any]],
    boundary: int,
) -> int:
    """把越界记录单独隔离到拒绝队列；按 (stream, lsn, reason) 幂等。"""
    by_event: dict[str, dict[str, Any]] = {}
    by_record: dict[str, dict[str, Any]] = {}
    for item in violations:
        event_id = item.get("event_id")
        source = item.get("source")
        column_detail = f"{item.get('column')}={item.get('value')!r} ({item.get('detail')})"
        if event_id and source == "event":
            bucket = by_event.setdefault(
                event_id,
                {"lsn": item.get("lsn"), "pk": item.get("pk"), "columns": []},
            )
            bucket["columns"].append(column_detail)
        else:
            bucket = by_record.setdefault(
                str(source),
                {
                    "lsn": boundary,
                    "columns": [],
                    "pks": set(),
                },
            )
            bucket["columns"].append(f"{item.get('pk')}: {column_detail}")
            if item.get("pk"):
                bucket["pks"].add(str(item["pk"]))

    created = 0

    def _persist(
        event: CdcEvent | None,
        lsn: int,
        pk: Any,
        source: str,
        columns: list[str],
        payload: dict[str, Any],
    ) -> None:
        nonlocal created
        exists = (
            db.query(RejectedEvent)
            .filter(
                RejectedEvent.stream_id == stream.id,
                RejectedEvent.lsn == lsn,
                RejectedEvent.reason == RejectReason.OUT_OF_RANGE.value,
            )
            .one_or_none()
        )
        if exists is not None:
            return
        db.add(
            RejectedEvent(
                event_id=event.id if event is not None else None,
                stream_id=stream.id,
                lsn=lsn,
                reason=RejectReason.OUT_OF_RANGE.value,
                detail=(f"按新版本规则检查越界（{source}）：" + "；".join(columns))[:600],
                payload_json={
                    "op": event.op if event is not None else "update",
                    "pk_value": (event.pk_value if event is not None else pk) or pk,
                    "before": event.before_json if event is not None else None,
                    "after": event.after_json if event is not None else payload,
                    "source": source,
                    "violations": columns,
                },
                status=RejectStatus.PENDING.value,
            )
        )
        created += 1

    for event_id, info in by_event.items():
        event = db.get(CdcEvent, event_id)
        if event is None:
            continue
        lsn = int(info["lsn"] if info["lsn"] is not None else event.lsn)
        _persist(
            event,
            lsn,
            info.get("pk"),
            "event",
            info["columns"],
            {"after": event.after_json, "pk_value": event.pk_value},
        )

    for source, info in by_record.items():
        lsn = int(info["lsn"] if info["lsn"] is not None else boundary)
        pks = sorted(info["pks"])
        _persist(
            None,
            lsn,
            pks,
            source,
            info["columns"],
            {"pk_value": pks, "source": source, "affected_pks": pks},
        )

    db.flush()
    return created


def _restore_state(
    db: Session,
    stream_id: Any,
    version_states: list[tuple[Any, str, int | None, datetime | None]],
    watermark_before: int,
) -> None:
    stream = db.get(ChangeStream, stream_id)
    if stream is not None:
        stream.watermark_lsn = int(watermark_before)
    for version_id, status, effective_from, published_at in version_states:
        version = db.get(SchemaVersion, version_id)
        if version is None:
            continue
        version.status = status
        version.effective_from_lsn = effective_from
        version.published_at = published_at


def _apply_change(
    db: Session,
    stream: ChangeStream,
    previous: SchemaVersion | None,
    version: SchemaVersion,
) -> dict[str, Any]:
    """执行可安全原地完成的 DDL；不支持的类型只记录，不当作已应用。"""
    target = _target_table(stream)
    table = f"{stream.name}_dw"
    inspector = inspect(db.get_bind())
    if not inspector.has_table(table, schema=settings.target_schema):
        return {"applied": False, "target_table": target, "statements": [], "reason": "目标表不存在"}
    if previous is None:
        return {"applied": False, "target_table": target, "statements": [], "reason": "缺少前序版本，未执行 DDL"}

    diff = schema_engine.diff_schemas(
        _version_payload(stream, previous),
        _version_payload(stream, version),
        target,
    )
    statements: list[str] = []
    skipped: list[str] = []
    table_sql = f"{_q(settings.target_schema)}.{_q(table)}"

    for change in diff.get("changes") or []:
        kind = change.get("kind")
        column = str(change.get("column") or "")
        old = change.get("old") or {}
        new = change.get("new") or {}
        col = _q(column)
        if kind == "add_nullable":
            statements.append(f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS {col} {new.get('type')};")
        elif kind == "add_required_with_default":
            statements.append(
                f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS {col} {new.get('type')} "
                f"NOT NULL DEFAULT {schema_engine._literal(new.get('default'))};"
            )
        elif kind == "add_required":
            statements.append(f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS {col} {new.get('type')};")
        elif kind == "drop_column":
            statements.append(f"ALTER TABLE {table_sql} DROP COLUMN IF EXISTS {col};")
        elif kind in ("type_widen", "type_narrow"):
            statements.append(
                f"ALTER TABLE {table_sql} ALTER COLUMN {col} TYPE {new.get('type')} "
                f"USING {col}::{schema_engine._base_type(new.get('type'))};"
            )
        elif kind == "nullability_to_required":
            statements.append(f"ALTER TABLE {table_sql} ALTER COLUMN {col} SET NOT NULL;")
        elif kind == "nullability_to_nullable":
            statements.append(f"ALTER TABLE {table_sql} ALTER COLUMN {col} DROP NOT NULL;")
        else:
            skipped.append(f"{kind}:{column}")

    for statement in statements:
        db.execute(text(statement))

    result: dict[str, Any] = {
        "applied": bool(statements),
        "target_table": target,
        "statements": statements,
        "skipped": skipped,
        "migration_sql": diff.get("migration_sql"),
        "digest": diff.get("digest"),
    }
    if skipped:
        result["reason"] = "部分破坏性变更需人工迁移：" + ", ".join(skipped)
    return result


def publish(
    db: Session,
    stream_id: str,
    version_id: str,
    effective_from_lsn: int,
    triggered_by: str | None = None,
    simulate_failure: bool = False,
) -> dict[str, Any]:
    stream = db.get(ChangeStream, stream_id)
    if stream is None:
        raise not_found("ChangeStream", stream_id)
    version = db.get(SchemaVersion, version_id)
    if version is None:
        raise AppError(codes.SCHEMA_VERSION_MISSING, "schema 版本不存在", f"version_id={version_id}")
    if str(version.stream_id) != str(stream.id):
        raise AppError(codes.VALIDATION_ERROR, "版本不属于该变更流", f"version={version.version_label}")
    if effective_from_lsn is None or int(effective_from_lsn) < 1:
        raise AppError(
            codes.SCHEMA_BOUNDARY_INVALID,
            "生效位点必须 >= 1",
            f"effective_from_lsn={effective_from_lsn}",
        )
    if version.status == SchemaVersionStatus.ACTIVE.value:
        raise AppError(
            codes.SCHEMA_VERSION_ACTIVE,
            "该版本已生效，不能重复发布",
            f"version={version.version_label} boundary={version.effective_from_lsn}",
        )

    boundary = int(effective_from_lsn)
    versions = (
        db.query(SchemaVersion)
        .filter(SchemaVersion.stream_id == stream.id)
        .order_by(SchemaVersion.created_at.asc())
        .all()
    )
    previous = _previous_version(versions, boundary)
    watermark_before = int(stream.watermark_lsn)
    version_states = [
        (item.id, item.status, item.effective_from_lsn, item.published_at) for item in versions
    ]

    precheck_result = precheck(db, stream, version, boundary)
    quarantined = _quarantine(db, stream, precheck_result["violations"], boundary)
    out_of_range = precheck_result["violation_count"] > 0

    base_kwargs = {
        "stream_id": stream.id,
        "version_id": version.id,
        "previous_version_id": previous.id if previous else None,
        "effective_from_lsn": boundary,
        "precheck_json": precheck_result,
        "quarantined_count": quarantined,
        "watermark_before": watermark_before,
        "triggered_by": triggered_by,
    }

    if out_of_range:
        db.flush()
        error = (
            f"发布前预检发现 {precheck_result['violation_count']} 处越界，"
            f"涉及 {precheck_result['affected_count']} 个订单；坏记录已隔离到拒绝队列"
        )
        _restore_state(db, stream.id, version_states, watermark_before)
        row = SchemaPublish(
            **base_kwargs,
            status=SchemaPublishStatus.FAILED.value,
            apply_json={"applied": False, "reason": "precheck_out_of_range"},
            rollback_json=_rollback_payload(previous, watermark_before, "precheck_out_of_range"),
            watermark_after=int(stream.watermark_lsn),
            error_message=error,
            finished_at=_now(),
        )
        db.add(row)
        db.commit()
        return serialize_publish(db, row)

    apply_result: dict[str, Any] = {"applied": False, "reason": "not_applied"}
    try:
        with db.begin_nested():
            if simulate_failure:
                raise RuntimeError("模拟发布失败：用于验证版本与处理位置回退")
            apply_result = _apply_change(db, stream, previous, version)
            version.status = SchemaVersionStatus.ACTIVE.value
            version.effective_from_lsn = boundary
            version.published_at = _now()
            for other in versions:
                if other.id == version.id or other.effective_from_lsn is None:
                    continue
                if (
                    int(other.effective_from_lsn) >= boundary
                    and other.status == SchemaVersionStatus.ACTIVE.value
                ):
                    other.status = SchemaVersionStatus.SUPERSEDED.value
    except Exception as exc:  # noqa: BLE001 - 失败必须落库并回退
        _restore_state(db, stream.id, version_states, watermark_before)
        db.flush()
        row = SchemaPublish(
            **base_kwargs,
            status=SchemaPublishStatus.ROLLED_BACK.value,
            apply_json=apply_result,
            rollback_json=_rollback_payload(
                previous,
                watermark_before,
                "simulated_failure" if simulate_failure else "apply_failed",
            ),
            watermark_after=int(watermark_before),
            error_message=f"发布失败已回退：{exc}"[:600],
            finished_at=_now(),
        )
        db.add(row)
        db.commit()
        return serialize_publish(db, row)

    row = SchemaPublish(
        **base_kwargs,
        status=SchemaPublishStatus.PUBLISHED.value,
        apply_json=apply_result,
        rollback_json={
            "performed": False,
            "reason": None,
            "restored_version_id": None,
            "restored_version_label": None,
            "restored_effective_from_lsn": None,
            "restored_watermark_lsn": None,
        },
        watermark_after=int(stream.watermark_lsn),
        error_message=None,
        finished_at=_now(),
    )
    db.add(row)
    db.commit()
    return serialize_publish(db, row)


def _rollback_payload(
    previous: SchemaVersion | None, watermark_before: int, reason: str
) -> dict[str, Any]:
    return {
        "performed": True,
        "reason": reason,
        "restored_version_id": str(previous.id) if previous else None,
        "restored_version_label": previous.version_label if previous else None,
        "restored_effective_from_lsn": int(previous.effective_from_lsn)
        if previous and previous.effective_from_lsn is not None
        else None,
        "restored_watermark_lsn": int(watermark_before),
        "restored_watermark_display": format_lsn(int(watermark_before)),
    }


def precheck_only(
    db: Session, stream_id: str, version_id: str, effective_from_lsn: int
) -> dict[str, Any]:
    stream = db.get(ChangeStream, stream_id)
    if stream is None:
        raise not_found("ChangeStream", stream_id)
    version = db.get(SchemaVersion, version_id)
    if version is None:
        raise AppError(codes.SCHEMA_VERSION_MISSING, "schema 版本不存在", f"version_id={version_id}")
    if str(version.stream_id) != str(stream.id):
        raise AppError(codes.VALIDATION_ERROR, "版本不属于该变更流", f"version={version.version_label}")
    if effective_from_lsn is None or int(effective_from_lsn) < 1:
        raise AppError(codes.SCHEMA_BOUNDARY_INVALID, "生效位点必须 >= 1", f"lsn={effective_from_lsn}")
    result = precheck(db, stream, version, int(effective_from_lsn))
    result["version"] = serialize_schema_version(version)
    result["stream"] = {"id": str(stream.id), "name": stream.name}
    return result


def serialize_publish(db: Session, row: SchemaPublish) -> dict[str, Any]:
    stream = db.get(ChangeStream, row.stream_id)
    version = db.get(SchemaVersion, row.version_id)
    previous = db.get(SchemaVersion, row.previous_version_id) if row.previous_version_id else None
    from src.constants.enums import ENUM_LABELS

    return {
        "id": str(row.id),
        "stream_id": str(row.stream_id),
        "stream_name": stream.name if stream else None,
        "version_id": str(row.version_id),
        "version_label": version.version_label if version else None,
        "previous_version_id": str(row.previous_version_id) if row.previous_version_id else None,
        "previous_version_label": previous.version_label if previous else None,
        "effective_from_lsn": int(row.effective_from_lsn),
        "effective_from_display": format_lsn(int(row.effective_from_lsn)),
        "status": row.status,
        "status_label": ENUM_LABELS["SchemaPublishStatus"].get(row.status, row.status),
        "precheck": row.precheck_json or {},
        "apply": row.apply_json or {},
        "rollback": row.rollback_json or {},
        "quarantined_count": int(row.quarantined_count),
        "watermark_before": int(row.watermark_before),
        "watermark_after": int(row.watermark_after),
        "watermark_after_display": format_lsn(int(row.watermark_after)),
        "error_message": row.error_message,
        "triggered_by": str(row.triggered_by) if row.triggered_by else None,
        "created_at": iso(row.created_at),
        "finished_at": iso(row.finished_at),
    }


def list_publishes(db: Session, stream_id: str | None = None, limit: int = 20) -> dict[str, Any]:
    query = db.query(SchemaPublish).order_by(SchemaPublish.created_at.desc())
    if stream_id:
        query = query.filter(SchemaPublish.stream_id == stream_id)
    rows = query.limit(limit).all()
    return {"items": [serialize_publish(db, row) for row in rows], "total": len(rows)}
