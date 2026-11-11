"""schema 演进 controller。"""
from __future__ import annotations

from fastapi import Request
from sqlalchemy.orm import Session

from src.models.user import User
from src.schemas.schema_diff import (
    SchemaDiffRequest,
    SchemaPrecheckRequest,
    SchemaPublishPathRequest,
    SchemaPublishRequest,
)
from src.services import audit_service, schema_publish_service, schema_service, stream_service


def list_versions(stream_id: str, db: Session) -> dict:
    stream_service.get_stream(db, stream_id)
    return schema_service.list_versions(db, stream_id) | {
        "pairs": schema_service.version_compare_pairs(db, stream_id)
    }


def precheck(request: Request, payload: SchemaPrecheckRequest, db: Session) -> dict:
    return schema_publish_service.precheck_only(
        db, payload.stream_id, payload.version_id, payload.effective_from_lsn
    )


def publish(request: Request, payload: SchemaPublishRequest, user: User, db: Session) -> dict:
    result = schema_publish_service.publish(
        db,
        payload.stream_id,
        payload.version_id,
        payload.effective_from_lsn,
        triggered_by=str(user.id),
        simulate_failure=payload.simulate_failure,
    )
    audit_service.record(
        db,
        action="schema.publish",
        entity_type="SchemaVersion",
        entity_id=payload.version_id,
        actor_id=str(user.id),
        payload={
            "stream_id": payload.stream_id,
            "status": result["status"],
            "effective_from_lsn": result["effective_from_lsn"],
            "affected_orders": result["precheck"].get("affected_count", 0),
            "rollback": bool((result.get("rollback") or {}).get("performed")),
        },
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return result


def list_publishes(stream_id: str | None, limit: int, db: Session) -> dict:
    return schema_publish_service.list_publishes(db, stream_id=stream_id, limit=limit)


def effective_version(stream_id: str, lsn: int, db: Session) -> dict:
    stream_service.get_stream(db, stream_id)
    return schema_service.effective_version(db, stream_id, lsn)


def publish_path(
    request: Request,
    stream_id: str,
    version_id: str,
    payload: SchemaPublishPathRequest,
    user: User,
    db: Session,
) -> dict:
    return publish(
        request,
        SchemaPublishRequest(
            stream_id=stream_id,
            version_id=version_id,
            effective_from_lsn=payload.effective_from_lsn,
            simulate_failure=payload.simulate_failure,
        ),
        user,
        db,
    )


def precheck_path(
    request: Request,
    stream_id: str,
    version_id: str,
    payload: SchemaPublishPathRequest,
    db: Session,
) -> dict:
    return precheck(
        request,
        SchemaPrecheckRequest(
            stream_id=stream_id,
            version_id=version_id,
            effective_from_lsn=payload.effective_from_lsn,
        ),
        db,
    )


def diff_versions(request: Request, payload: SchemaDiffRequest, user: User, db: Session) -> dict:
    result = schema_service.diff(db, payload.left_version_id, payload.right_version_id)
    audit_service.record(
        db,
        action="schema.diff",
        entity_type="SchemaVersion",
        entity_id=f"{payload.left_version_id}->{payload.right_version_id}",
        actor_id=str(user.id),
        payload={
            "compat_level": result["compat_level"],
            "change_count": len(result["changes"]),
        },
        ip=getattr(request.state, "client_ip", None),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return result
