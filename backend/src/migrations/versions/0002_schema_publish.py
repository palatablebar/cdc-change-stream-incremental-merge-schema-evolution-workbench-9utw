"""schema 版本生效边界 + 发布尝试记录。

Revision ID: 0002_schema_publish
"""
from __future__ import annotations

from alembic import op

from src.config.settings import settings
from src.models import Base

revision = "0002_schema_publish"
down_revision = "0001_initial"
branch_labels = None
depends_on = None

CDC = settings.cdc_schema


def upgrade() -> None:
    bind = op.get_bind()
    # 新表由模型创建；已存在的库再通过 IF NOT EXISTS 补列，两种路径都幂等。
    Base.metadata.create_all(bind=bind)
    op.execute(
        f"ALTER TABLE {CDC}.schema_versions "
        "ADD COLUMN IF NOT EXISTS effective_from_lsn BIGINT"
    )
    op.execute(
        f"ALTER TABLE {CDC}.schema_versions "
        "ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'draft'"
    )
    op.execute(
        f"ALTER TABLE {CDC}.schema_versions "
        "ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ"
    )


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS {CDC}.schema_publishes")
    op.execute(f"ALTER TABLE {CDC}.schema_versions DROP COLUMN IF EXISTS published_at")
    op.execute(f"ALTER TABLE {CDC}.schema_versions DROP COLUMN IF EXISTS status")
    op.execute(f"ALTER TABLE {CDC}.schema_versions DROP COLUMN IF EXISTS effective_from_lsn")
