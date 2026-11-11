from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from src.config.database import get_db
from src.controllers import schema_controller
from src.middlewares.rbac import require_permission
from src.models.user import User
from src.schemas.schema_diff import (
    SchemaDiffRequest,
    SchemaPrecheckRequest,
    SchemaPublishPathRequest,
    SchemaPublishRequest,
)

router = APIRouter(tags=["schema"])


@router.get("/streams/{stream_id}/schema-versions")
def list_versions(
    stream_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:read")),
) -> dict:
    return schema_controller.list_versions(stream_id, db)


@router.get("/streams/{stream_id}/schema-versions/effective")
def effective_version(
    stream_id: str,
    lsn: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:read")),
) -> dict:
    return schema_controller.effective_version(stream_id, lsn, db)


@router.get("/schema/effective-version")
def effective_version_query(
    stream_id: str,
    lsn: int,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:read")),
) -> dict:
    return schema_controller.effective_version(stream_id, lsn, db)


@router.post("/schema/diff")
def diff_versions(
    request: Request,
    payload: SchemaDiffRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:read")),
) -> dict:
    return schema_controller.diff_versions(request, payload, user, db)


@router.post("/schema/precheck")
def precheck_version(
    request: Request,
    payload: SchemaPrecheckRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:write")),
) -> dict:
    return schema_controller.precheck(request, payload, db)


@router.post("/schema/publish")
def publish_version(
    request: Request,
    payload: SchemaPublishRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:write")),
) -> dict:
    return schema_controller.publish(request, payload, user, db)


@router.get("/schema/publishes")
def list_publishes(
    stream_id: str | None = None,
    limit: int = 20,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:read")),
) -> dict:
    return schema_controller.list_publishes(stream_id, min(limit, 100), db)


@router.get("/streams/{stream_id}/schema-publishes")
def list_stream_publishes(
    stream_id: str,
    limit: int = 20,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:read")),
) -> dict:
    return schema_controller.list_publishes(stream_id, min(limit, 100), db)


@router.post("/streams/{stream_id}/schema-versions/{version_id}/precheck")
def precheck_path(
    request: Request,
    stream_id: str,
    version_id: str,
    payload: SchemaPublishPathRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:write")),
) -> dict:
    return schema_controller.precheck_path(request, stream_id, version_id, payload, db)


@router.post("/streams/{stream_id}/schema-versions/{version_id}/publish")
def publish_path(
    request: Request,
    stream_id: str,
    version_id: str,
    payload: SchemaPublishPathRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("schema:write")),
) -> dict:
    return schema_controller.publish_path(request, stream_id, version_id, payload, user, db)
