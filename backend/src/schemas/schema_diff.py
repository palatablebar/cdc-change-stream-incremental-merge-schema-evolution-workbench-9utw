from __future__ import annotations

from pydantic import BaseModel, Field


class SchemaDiffRequest(BaseModel):
    left_version_id: str
    right_version_id: str


class SchemaPublishRequest(BaseModel):
    stream_id: str
    version_id: str
    effective_from_lsn: int = Field(ge=1)
    simulate_failure: bool = False


class SchemaPrecheckRequest(BaseModel):
    stream_id: str
    version_id: str
    effective_from_lsn: int = Field(ge=1)


class SchemaPublishPathRequest(BaseModel):
    """REST 别名：stream/version 放在路径里时只需传边界与演练开关。"""

    effective_from_lsn: int = Field(ge=1)
    simulate_failure: bool = False


def serialize_schema_version(version: Any) -> dict:
    from src.constants.enums import ENUM_LABELS
    from src.utils.formatters import format_lsn, iso

    effective_from = getattr(version, "effective_from_lsn", None)
    status = getattr(version, "status", "draft") or "draft"
    return {
        "id": str(version.id),
        "stream_id": str(version.stream_id),
        "version_label": version.version_label,
        "fields": version.fields_json,
        "field_count": len(version.fields_json or []),
        "ddl_summary": version.ddl_summary,
        "effective_from_lsn": effective_from,
        "effective_from_display": format_lsn(effective_from) if effective_from is not None else None,
        "effective_to_lsn": None,
        "status": status,
        "status_label": ENUM_LABELS["SchemaVersionStatus"].get(status, status),
        "published_at": iso(getattr(version, "published_at", None)),
        "created_by": version.created_by,
        "created_at": iso(version.created_at),
    }
