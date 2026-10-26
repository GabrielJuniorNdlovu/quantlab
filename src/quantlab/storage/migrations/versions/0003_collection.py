"""Collection process state reuses immutable batch receipts as durable checkpoints."""

from alembic import op

revision = "0003_collection"
down_revision = "0002_storage_guards"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.get_bind().exec_driver_sql("""
    CREATE TABLE collection_sessions (
      capture_id UUID PRIMARY KEY REFERENCES acquisition_runs(capture_id),
      status TEXT NOT NULL CHECK(status IN ('ACTIVE','PAUSED','FAILED','COMPLETE')),
      settings JSONB NOT NULL,
      metrics JSONB NOT NULL DEFAULT '{}',
      recovery_attempts BIGINT NOT NULL DEFAULT 0 CHECK(recovery_attempts>=0),
      last_error TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );
    CREATE TRIGGER collection_removal_guard BEFORE DELETE ON collection_sessions
      FOR EACH ROW EXECUTE FUNCTION forbid_evidence_removal();
    CREATE TRIGGER collection_truncate_guard BEFORE TRUNCATE ON collection_sessions
      EXECUTE FUNCTION forbid_evidence_removal();
    """)


def downgrade() -> None:
    raise RuntimeError("collection evidence requires an explicitly reviewed migration")
