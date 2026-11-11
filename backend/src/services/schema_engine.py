"""算法 2：schema 版本兼容性判定 + 迁移 SQL 生成（纯函数，无 IO）。

输入：两个 SchemaVersion 的字段集合（name/type/nullable/default/is_pk）。
处理：
  1. 字段级比较 → 变更类别（ChangeKind 枚举，10 类）；
  2. 每类按兼容矩阵给出方向（backward / forward / breaking）；
  3. 汇总等级取最差；只有 backward + forward 混合时判 breaking（方向冲突）；
  4. 为每个 breaking/backward 变更生成前滚 SQL、预检 SQL、回滚 SQL 与发布顺序建议。

口径（写进 README，不是随意的）：
  backward = 新 schema 能读旧数据（可以直接发新版本）
  forward  = 旧 schema 能读新数据（需要下游先升级）
  breaking = 两个方向都不成立（必须先双写/回填/切读）
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from src.constants.enums import ChangeKind, CompatLevel, SchemaVersionStatus

# 变更类别 -> 兼容方向
CHANGE_COMPAT: dict[str, str] = {
    ChangeKind.ADD_NULLABLE.value: CompatLevel.BACKWARD.value,
    ChangeKind.ADD_REQUIRED.value: CompatLevel.BREAKING.value,
    ChangeKind.ADD_REQUIRED_WITH_DEFAULT.value: CompatLevel.BACKWARD.value,
    ChangeKind.DROP_COLUMN.value: CompatLevel.BREAKING.value,
    ChangeKind.TYPE_WIDEN.value: CompatLevel.BACKWARD.value,
    ChangeKind.TYPE_NARROW.value: CompatLevel.BREAKING.value,
    ChangeKind.TYPE_INCOMPATIBLE.value: CompatLevel.BREAKING.value,
    ChangeKind.NULLABILITY_TO_NULLABLE.value: CompatLevel.BACKWARD.value,
    ChangeKind.NULLABILITY_TO_REQUIRED.value: CompatLevel.BREAKING.value,
    ChangeKind.PK_CHANGE.value: CompatLevel.BREAKING.value,
}

# 数值族的容量排序：数值越大越"宽"
INTEGER_FAMILY = {"smallint": 1, "integer": 2, "int": 2, "bigint": 3}
NUMERIC_FAMILY = {"numeric", "decimal"}
FLOAT_FAMILY = {"real", "double", "double precision", "float"}
TEXT_FAMILY = {"text", "varchar", "character varying", "char", "character"}
TEMPORAL_FAMILY = {"date", "timestamp", "timestamptz", "timestamp with time zone", "timestamp without time zone"}
BOOL_FAMILY = {"boolean", "bool"}
JSON_FAMILY = {"json", "jsonb"}


def parse_type(raw: str) -> tuple[str, tuple[int, ...]]:
    """拆出 (基础类型, 参数元组)。varchar(80) -> ('varchar', (80,))。"""
    text = (raw or "text").strip().lower()
    if "(" in text and text.endswith(")"):
        base, params = text.split("(", 1)
        params = params.rstrip(")")
        try:
            nums = tuple(int(part.strip()) for part in params.split(",") if part.strip())
        except ValueError:
            nums = ()
        return base.strip(), nums
    return text, ()


def family_of(base: str) -> str:
    if base in INTEGER_FAMILY:
        return "integer"
    if base in NUMERIC_FAMILY:
        return "numeric"
    if base in FLOAT_FAMILY:
        return "float"
    if base in TEXT_FAMILY:
        return "text"
    if base in TEMPORAL_FAMILY:
        return "temporal"
    if base in BOOL_FAMILY:
        return "boolean"
    if base in JSON_FAMILY:
        return "json"
    return "unknown"


# ---------------------------------------------------------------------------
# 按「事件自己的位点」选择版本 / 用真实值检查越界
# ---------------------------------------------------------------------------

INTEGER_BOUNDS: dict[str, tuple[Decimal, Decimal]] = {
    "smallint": (Decimal("-32768"), Decimal("32767")),
    "integer": (Decimal("-2147483648"), Decimal("2147483647")),
    "int": (Decimal("-2147483648"), Decimal("2147483647")),
    "bigint": (Decimal("-9223372036854775808"), Decimal("9223372036854775807")),
}

_NUMERIC_NOISE = re.compile(r"[¥$,\s]")


def _to_decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool):
        return Decimal(int(value))
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except InvalidOperation:  # pragma: no cover - str(int/float) 总能解析
            return None
    if isinstance(value, str):
        text = _NUMERIC_NOISE.sub("", value)
        try:
            return Decimal(text)
        except InvalidOperation:
            return None
    return None


def numeric_limits(sql_type: str) -> tuple[Decimal | None, Decimal | None, int]:
    """返回 (最小值, 最大值, 小数位)。非数值类型返回 (None, None, 0)。"""
    base, params = parse_type(sql_type)
    if base in NUMERIC_FAMILY:
        if not params:
            return None, None, 0
        precision = params[0]
        scale = params[1] if len(params) > 1 else 0
        if precision <= 0:
            return None, None, scale
        integer_digits = max(0, precision - scale)
        unit = Decimal(1).scaleb(-scale)
        max_abs = (Decimal(10) ** integer_digits) - unit
        return -max_abs, max_abs, scale
    if base in INTEGER_BOUNDS:
        low, high = INTEGER_BOUNDS[base]
        return low, high, 0
    return None, None, 0


def _format_decimal(value: Decimal | None) -> str:
    if value is None:
        return "NULL"
    return format(value, "f")


def _json_value(value: Any) -> Any:
    """把预检明细里的值转成可写入 JSONB / 返回前端的形式。"""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)


def scalar_violation(value: Any, sql_type: str) -> dict[str, str] | None:
    """检查单个已存在的值是否满足目标列定义。

    返回 {"reason": RejectReason 值, "detail": 说明}；合法返回 None。
    这是「用真实记录做发布前预检」与「归并没条按位点选规则」共用的纯函数。
    """
    if value is None:
        return None
    base, params = parse_type(sql_type)
    family = family_of(base)

    if family in ("numeric", "integer"):
        number = _to_decimal(value)
        if number is None:
            return {
                "reason": "type_conflict",
                "detail": f"值 {value!r} 无法解析为 {sql_type}",
            }
        if not number.is_finite():
            return {
                "reason": "type_conflict",
                "detail": f"值 {value!r} 不是有限数值，无法写入 {sql_type}",
            }
        low, high, scale = numeric_limits(sql_type)
        if low is not None and high is not None and (number < low or number > high):
            return {
                "reason": "out_of_range",
                "detail": f"值 {value!r} 超出 {sql_type} 允许范围 [{_format_decimal(low)}, {_format_decimal(high)}]",
            }
        if family == "integer" and number != number.to_integral_value():
            return {
                "reason": "type_conflict",
                "detail": f"值 {value!r} 不是整数，无法写入 {sql_type}",
            }
        if scale and number.as_tuple().exponent < -scale:
            quantized = number.quantize(Decimal(1).scaleb(-scale))
            if quantized != number:
                return {
                    "reason": "out_of_range",
                    "detail": f"值 {value!r} 的小数位超过 {sql_type} 的 scale={scale}",
                }
        return None

    if family == "text" and params and base not in ("text",):
        if len(str(value)) > params[0]:
            return {
                "reason": "out_of_range",
                "detail": f"值长度 {len(str(value))} 超出 {sql_type} 上限 {params[0]}",
            }
    return None


def value_violation(value: Any, field: dict[str, Any]) -> dict[str, str] | None:
    if value is None:
        if not bool(field.get("nullable", True)):
            return {
                "reason": "schema_mismatch",
                "detail": f"必填列 {field.get('name')} 不允许为 null",
            }
        return None
    return scalar_violation(value, str(field.get("type", "text")))


def validate_record(
    values: dict[str, Any] | None,
    fields: list[dict[str, Any]],
    require_all: bool = True,
) -> list[dict[str, Any]]:
    """按给定版本字段校验一条真实记录，返回越界/缺失明细。

    require_all=False 时只校验载荷里实际出现的字段（CDC update 可能只带变化列）。
    """
    payload = values or {}
    violations: list[dict[str, Any]] = []
    for field in fields or []:
        name = str(field.get("name", ""))
        if not name:
            continue
        if name not in payload:
            if (
                require_all
                and not bool(field.get("nullable", True))
                and field.get("default") is None
            ):
                violations.append(
                    {
                        "column": name,
                        "value": None,
                        "type": field.get("type"),
                        "reason": "schema_mismatch",
                        "detail": f"缺少必填列 {name}",
                    }
                )
            continue
        violation = value_violation(payload.get(name), field)
        if violation:
            violations.append(
                {
                    "column": name,
                    "value": _json_value(payload.get(name)),
                    "type": field.get("type"),
                    **violation,
                }
            )
    return violations


def _get(version: Any, key: str, default: Any = None) -> Any:
    if isinstance(version, dict):
        return version.get(key, default)
    return getattr(version, key, default)


def _is_effective(version: Any) -> bool:
    if _get(version, "effective_from_lsn") is None:
        return False
    status = _get(version, "status")
    if status in (None, ""):
        return True
    return status not in (
        SchemaVersionStatus.SUPERSEDED.value,
        SchemaVersionStatus.ROLLED_BACK.value,
    )


def resolve_version_for_lsn(versions: list[Any], lsn: int) -> Any | None:
    """按事件位点选择生效版本：取 effective_from_lsn <= lsn 的最新版本。

    - 老版本在边界之前继续生效，新版本从边界（含）开始生效；
    - 未发布（draft）/已回退（rolled_back）的版本不参与选择；
    - 如果没有任何版本在 lsn 之前生效，退回最早登记的版本，保证老事件仍有规则可用。
    """
    ordered = sorted(
        versions or [],
        key=lambda item: (
            int(_get(item, "effective_from_lsn") or 0),
            str(_get(item, "created_at") or ""),
            str(_get(item, "version_label") or ""),
            str(_get(item, "id") or ""),
        ),
    )
    eligible = [version for version in ordered if _is_effective(version)]
    if not eligible:
        return ordered[0] if ordered else None
    selected = None
    for version in eligible:
        if int(_get(version, "effective_from_lsn")) <= int(lsn):
            selected = version
        else:
            break
    if selected is not None:
        return selected
    # 位点早于所有已发布边界：优先退回最早登记的「原始结构」（未设置边界的版本）。
    baseline = [
        version
        for version in ordered
        if _get(version, "effective_from_lsn") is None
        and _get(version, "status") != SchemaVersionStatus.ROLLED_BACK.value
    ]
    if baseline:
        return baseline[0]
    return eligible[0] if eligible else (ordered[0] if ordered else None)


def classify_type_change(old_type: str, new_type: str) -> str:
    old_base, old_params = parse_type(old_type)
    new_base, new_params = parse_type(new_type)
    if old_base == new_base and old_params == new_params:
        return "same"

    old_family = family_of(old_base)
    new_family = family_of(new_base)

    if old_family == new_family == "integer":
        return "widen" if INTEGER_FAMILY[new_base] > INTEGER_FAMILY[old_base] else "narrow"

    if old_family == new_family == "numeric":
        old_p = old_params[0] if old_params else 38
        new_p = new_params[0] if new_params else 38
        old_s = old_params[1] if len(old_params) > 1 else 0
        new_s = new_params[1] if len(new_params) > 1 else 0
        if new_p >= old_p and new_s >= old_s and new_p > old_p:
            return "widen"
        if new_p == old_p and new_s == old_s:
            return "same"
        return "narrow"

    if old_family == new_family == "text":
        old_len = old_params[0] if old_params else 0
        new_len = new_params[0] if new_params else 0
        if old_base == "text" or new_base == "text":
            return "same" if old_base == new_base else ("widen" if new_base == "text" else "narrow")
        if new_len >= old_len and new_len > old_len:
            return "widen"
        if new_len == old_len:
            return "same"
        return "narrow"

    if old_family == new_family == "temporal":
        rank = {"date": 0, "timestamp": 1, "timestamptz": 2, "timestamp without time zone": 1, "timestamp with time zone": 2}
        if rank.get(new_base, 0) > rank.get(old_base, 0):
            return "widen"
        if rank.get(new_base, 0) == rank.get(old_base, 0):
            return "same"
        return "narrow"

    if old_family == new_family:
        return "same"

    # 跨族：整数可以安全放进 numeric/float；反过来是收窄；文本与其它族不兼容
    if old_family == "integer" and new_family in ("numeric", "float"):
        return "widen"
    if old_family in ("numeric", "float") and new_family == "integer":
        return "narrow"
    if old_family == "numeric" and new_family == "float":
        return "narrow"
    if old_family == "float" and new_family == "numeric":
        return "narrow"
    if old_family == "json" and new_family == "json":
        return "same"
    if old_family == "date" and new_family == "temporal":
        return "widen"
    return "incompatible"


def _field_map(fields: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(field["name"]): dict(field) for field in fields}


def diff_schemas(
    left: dict[str, Any],
    right: dict[str, Any],
    target_table: str,
    dialect: str = "postgresql",
) -> dict[str, Any]:
    """left = 旧版本，right = 新版本。"""
    left_fields = _field_map(left.get("fields") or [])
    right_fields = _field_map(right.get("fields") or [])
    left_pks = list(left.get("pk_columns") or [])
    right_pks = list(right.get("pk_columns") or [])

    pk_changed = left_pks != right_pks

    changes: list[dict[str, Any]] = []
    order = list(left_fields.keys()) + [name for name in right_fields if name not in left_fields]

    for name in order:
        old = left_fields.get(name)
        new = right_fields.get(name)

        if old is not None and new is None:
            kind = ChangeKind.PK_CHANGE.value if old.get("is_pk") else ChangeKind.DROP_COLUMN.value
            changes.append(_make_change(kind, name, old, None))
            continue

        if old is None and new is not None:
            if new.get("is_pk"):
                kind = ChangeKind.PK_CHANGE.value
            elif not new.get("nullable", True):
                kind = (
                    ChangeKind.ADD_REQUIRED_WITH_DEFAULT.value
                    if new.get("default") is not None
                    else ChangeKind.ADD_REQUIRED.value
                )
            else:
                kind = ChangeKind.ADD_NULLABLE.value
            changes.append(_make_change(kind, name, None, new))
            continue

        if old is None or new is None:
            continue

        type_change = classify_type_change(str(old.get("type", "text")), str(new.get("type", "text")))
        if type_change == "widen":
            changes.append(_make_change(ChangeKind.TYPE_WIDEN.value, name, old, new))
        elif type_change == "narrow":
            changes.append(_make_change(ChangeKind.TYPE_NARROW.value, name, old, new))
        elif type_change == "incompatible":
            changes.append(_make_change(ChangeKind.TYPE_INCOMPATIBLE.value, name, old, new))

        old_nullable = bool(old.get("nullable", True))
        new_nullable = bool(new.get("nullable", True))
        if old_nullable and not new_nullable:
            changes.append(_make_change(ChangeKind.NULLABILITY_TO_REQUIRED.value, name, old, new))
        elif not old_nullable and new_nullable:
            changes.append(_make_change(ChangeKind.NULLABILITY_TO_NULLABLE.value, name, old, new))

    if pk_changed:
        pk_change_entry = {
            "kind": ChangeKind.PK_CHANGE.value,
            "kind_label": "主键变更",
            "column": ", ".join(left_pks) + " → " + ", ".join(right_pks),
            "old": {"pk_columns": left_pks},
            "new": {"pk_columns": right_pks},
            "compat": CompatLevel.BREAKING.value,
            "forward_sql": [
                f"-- 主键变更需要重建目标表（{dialect}）",
                f"-- CREATE TABLE {target_table}__new (LIKE {target_table} INCLUDING ALL, PRIMARY KEY ("
                + ", ".join(right_pks)
                + "));",
            ],
            "precheck_sql": [
                f"-- 预检：新旧主键组合唯一性",
                f"SELECT count(*) FROM {target_table} GROUP BY {', '.join(right_pks)} HAVING count(*) > 1;",
            ],
            "rollback_sql": [f"-- 回滚：切回旧表 {target_table}__bak"],
            "note": "主键变更必须走重建 + 双写窗口，无法原地 ALTER。",
        }
        changes.append(pk_change_entry)

    for change in changes:
        change["compat"] = CHANGE_COMPAT.get(change["kind"], CompatLevel.BREAKING.value)
        if "forward_sql" not in change:
            change.update(_sql_for(change, target_table, dialect))

    level = aggregate_level([c["compat"] for c in changes])
    counts = {kind: 0 for kind in ChangeKind.values()}
    for change in changes:
        counts[change["kind"]] += 1
    counts = {kind: value for kind, value in counts.items() if value}

    breaking = [c for c in changes if c["compat"] == CompatLevel.BREAKING.value]
    migration_sql = _assemble_forward(changes, target_table)
    rollback_sql = _assemble_rollback(changes, target_table)

    payload = {
        "left": {"version": left.get("version_label"), "field_count": len(left_fields), "pk_columns": left_pks},
        "right": {"version": right.get("version_label"), "field_count": len(right_fields), "pk_columns": right_pks},
        "changes": changes,
        "counts": counts,
        "compat_level": level,
        "breaking_count": len(breaking),
        "affected_columns": sorted({c["column"] for c in changes if c["column"]}),
        "migration_sql": migration_sql,
        "rollback_sql": rollback_sql,
        "deploy_steps": _deploy_steps(level, breaking, target_table),
    }
    payload["digest"] = "sha256:" + hashlib.sha256(
        json.dumps(
            {"left": payload["left"], "right": payload["right"], "changes": changes, "level": level},
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    return payload


def _make_change(kind: str, name: str, old: dict[str, Any] | None, new: dict[str, Any] | None) -> dict[str, Any]:
    from src.constants.enums import ENUM_LABELS

    return {
        "kind": kind,
        "kind_label": ENUM_LABELS["ChangeKind"].get(kind, kind),
        "column": name,
        "old": _field_projection(old),
        "new": _field_projection(new),
    }


def _field_projection(field: dict[str, Any] | None) -> dict[str, Any] | None:
    if field is None:
        return None
    return {
        "name": field.get("name"),
        "type": field.get("type"),
        "nullable": bool(field.get("nullable", True)),
        "default": field.get("default"),
        "is_pk": bool(field.get("is_pk")),
    }


def _sql_for(change: dict[str, Any], table: str, dialect: str) -> dict[str, Any]:
    kind = change["kind"]
    column = change["column"]
    old = change.get("old") or {}
    new = change.get("new") or {}
    forward: list[str] = []
    precheck: list[str] = []
    rollback: list[str] = []
    note = ""

    if kind == ChangeKind.ADD_NULLABLE.value:
        forward.append(f"ALTER TABLE {table} ADD COLUMN {column} {new.get('type')};")
        rollback.append(f"ALTER TABLE {table} DROP COLUMN {column};")
        note = "新增可空列，老读者忽略它即可，可直接发布。"
    elif kind == ChangeKind.ADD_REQUIRED_WITH_DEFAULT.value:
        forward.append(
            f"ALTER TABLE {table} ADD COLUMN {column} {new.get('type')} NOT NULL DEFAULT {_literal(new.get('default'))};"
        )
        forward.append(f"-- 默认值会回填历史行，确认回填量可接受后再发布")
        rollback.append(f"ALTER TABLE {table} DROP COLUMN {column};")
        note = "带默认值的必填列，历史行自动回填，属向后兼容。"
    elif kind == ChangeKind.ADD_REQUIRED.value:
        forward.append(f"-- 阻断变更：{column} 必填且无默认值，分三步发布")
        forward.append(f"ALTER TABLE {table} ADD COLUMN {column} {new.get('type')};")
        forward.append(f"UPDATE {table} SET {column} = <业务回填值> WHERE {column} IS NULL;")
        forward.append(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL;")
        precheck.append(f"SELECT count(*) FROM {table} WHERE {column} IS NULL;")
        rollback.append(f"ALTER TABLE {table} DROP COLUMN {column};")
        note = "新必填列无默认值，必须先回填再 SET NOT NULL。"
    elif kind == ChangeKind.DROP_COLUMN.value:
        forward.append(f"-- 破坏性变更：删列前先快照")
        forward.append(f"CREATE TABLE {table}__snapshot_{_stamp(change)} AS SELECT * FROM {table};")
        forward.append(f"ALTER TABLE {table} DROP COLUMN {column};")
        rollback.append(f"ALTER TABLE {table} ADD COLUMN {column} {old.get('type')};")
        rollback.append(f"-- 数据需从 {table}__snapshot_{_stamp(change)} 回灌")
        note = "删列会让下游读取该列的报表直接报错，必须与下游确认。"
    elif kind == ChangeKind.TYPE_WIDEN.value:
        forward.append(f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {new.get('type')};")
        rollback.append(
            f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {old.get('type')} USING {column}::{_base_type(old.get('type'))};"
        )
        precheck.append(f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL;")
        note = "类型放宽，旧数据可被新类型容纳。"
    elif kind == ChangeKind.TYPE_NARROW.value:
        precheck.append(_precheck_bounds_sql(table, column, str(new.get("type") or "")))
        forward.append(f"-- 收窄前必须确认没有越界值（预检为 0 才能继续）")
        forward.append(
            f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {new.get('type')} USING {column}::{_base_type(new.get('type'))};"
        )
        rollback.append(
            f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {old.get('type')} USING {column}::{_base_type(old.get('type'))};"
        )
        note = "类型收窄可能截断/溢出，属破坏性变更；预检会按真实值检查边界。"
    elif kind == ChangeKind.TYPE_INCOMPATIBLE.value:
        precheck.append(f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL;")
        forward.append(f"-- 类型不兼容：{old.get('type')} → {new.get('type')}，需新增列 + 回填 + 切读")
        forward.append(f"ALTER TABLE {table} ADD COLUMN {column}_new {new.get('type')};")
        forward.append(f"UPDATE {table} SET {column}_new = {column}::text::{_base_type(new.get('type'))};")
        rollback.append(f"ALTER TABLE {table} DROP COLUMN {column}_new;")
        note = "跨族类型无法原地转换，走影子列。"
    elif kind == ChangeKind.NULLABILITY_TO_REQUIRED.value:
        precheck.append(f"SELECT count(*) FROM {table} WHERE {column} IS NULL;")
        forward.append(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL;")
        rollback.append(f"ALTER TABLE {table} ALTER COLUMN {column} DROP NOT NULL;")
        note = "改为非空要求历史行无 NULL。"
    elif kind == ChangeKind.NULLABILITY_TO_NULLABLE.value:
        forward.append(f"ALTER TABLE {table} ALTER COLUMN {column} DROP NOT NULL;")
        rollback.append(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL;")
        note = "放宽为可空，向后兼容。"
    else:
        forward.append(f"-- 未识别变更 {kind}，需人工评审")
        note = "未识别的变更类别。"

    return {"forward_sql": forward, "precheck_sql": precheck, "rollback_sql": rollback, "note": note}


def _stamp(change: dict[str, Any]) -> str:
    return hashlib.sha1(change["column"].encode("utf-8")).hexdigest()[:6]


def _base_type(raw: str | None) -> str:
    base, _ = parse_type(raw or "text")
    return base


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _precheck_bounds_sql(table: str, column: str, sql_type: str) -> str:
    """生成「按真实值检查越界」的预检 SQL，而不是只数非空行。"""
    base, params = parse_type(sql_type)
    family = family_of(base)
    if family in ("numeric", "integer"):
        low, high, _ = numeric_limits(sql_type)
        if low is not None and high is not None:
            return (
                f"SELECT count(*) AS out_of_range FROM {table} WHERE {column} IS NOT NULL "
                f"AND ({column} < {_format_decimal(low)} OR {column} > {_format_decimal(high)});"
            )
    if family == "text" and params and base != "text":
        return (
            f"SELECT count(*) AS out_of_range FROM {table} WHERE {column} IS NOT NULL "
            f"AND length({column}) > {params[0]};"
        )
    return f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL;"


def aggregate_level(levels: list[str]) -> str:
    if not levels:
        return CompatLevel.FULL.value
    unique = set(levels)
    if CompatLevel.BREAKING.value in unique:
        return CompatLevel.BREAKING.value
    if CompatibilityMix.backward_forward_mixed(unique):
        return CompatLevel.BREAKING.value
    if CompatLevel.FORWARD.value in unique:
        return CompatLevel.FORWARD.value
    if CompatLevel.BACKWARD.value in unique:
        return CompatLevel.BACKWARD.value
    return CompatLevel.FULL.value


class CompatibilityMix:
    @staticmethod
    def backward_forward_mixed(unique: set[str]) -> bool:
        return CompatLevel.BACKWARD.value in unique and CompatLevel.FORWARD.value in unique


def _assemble_forward(changes: list[dict[str, Any]], table: str) -> str:
    if not changes:
        return f"-- 两个版本字段完全一致，无需迁移\nSELECT 1;"
    lines = [f"-- 目标表 {table}，按兼容等级分批发布", "BEGIN;"]
    for change in changes:
        if change.get("precheck_sql"):
            lines.append("-- 预检")
            lines.extend("-- " + sql for sql in change["precheck_sql"])
        lines.append(f"-- [{change['compat']}] {change.get('kind_label')} · {change['column']}")
        lines.extend(change.get("forward_sql") or [])
    lines.append("COMMIT;")
    return "\n".join(lines)


def _assemble_rollback(changes: list[dict[str, Any]], table: str) -> str:
    if not changes:
        return "-- 无变更，无需回滚"
    lines = [f"-- 回滚脚本（{table}），按变更逆序执行", "BEGIN;"]
    for change in reversed(changes):
        lines.append(f"-- 回滚 [{change.get('kind_label')}] {change['column']}")
        lines.extend(change.get("rollback_sql") or [])
    lines.append("COMMIT;")
    return "\n".join(lines)


def _deploy_steps(level: str, breaking: list[dict[str, Any]], table: str) -> list[str]:
    if level == CompatLevel.FULL.value:
        return ["两个版本字段一致，无需发布动作。"]
    if level == CompatLevel.BACKWARD.value:
        return [
            f"在 {table} 执行迁移 SQL；",
            "新版本上游可写、老版本上游可继续写；",
            "无需协调下游改代码。",
        ]
    columns = ", ".join(c["column"] for c in breaking if c.get("column")) or "—"
    return [
        f"标记受影响列：{columns}；",
        "先建快照表并备份受影响列；",
        "双写窗口内同时写新旧结构；",
        "回填历史数据并跑预检 SQL（结果为 0 才继续）；",
        "下游改完读取逻辑后切读，再删旧列/收紧类型；",
        "保留回滚脚本直到下一个发布周期。",
    ]
