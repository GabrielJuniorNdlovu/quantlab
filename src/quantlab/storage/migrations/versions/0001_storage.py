"""Canonical durable ingestion schema; downgrade intentionally preserves evidence."""

from alembic import op

revision = "0001_storage"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    from quantlab.storage.schema import SCHEMA

    op.get_bind().exec_driver_sql(SCHEMA)


def downgrade() -> None:
    raise RuntimeError("destructive downgrade refused; restore an explicitly reviewed backup")
