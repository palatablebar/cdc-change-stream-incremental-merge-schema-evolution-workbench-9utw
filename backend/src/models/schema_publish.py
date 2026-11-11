from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.config.settings import settings
from src.models.base import Base, new_uuid, utcnow


class SchemaPublish(Base):
    """一次 schema 发布尝试：预检、隔离、生效边界与失败回退结果都留痕。

    schema_versions 保留版本定义本身；发布是否成功、回退到哪个版本、
    水位线恢复到哪一位，都记在这张表里，页面据此展示发布历史。
    """

    __tablename__ = "schema_publishes"
    __table_args__ = {"schema": settings.cdc_schema}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    stream_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    previous_version_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    effective_from_lsn: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="failed", index=True)
    precheck_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    apply_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    rollback_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    quarantined_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    watermark_before: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    watermark_after: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(String(600), nullable=True)
    triggered_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
