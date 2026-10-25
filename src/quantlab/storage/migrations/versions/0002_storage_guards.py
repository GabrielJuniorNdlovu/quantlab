"""Harden publication and canonical source links without deleting stored evidence."""

from alembic import op

revision = "0002_storage_guards"
down_revision = "0001_storage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.get_bind().exec_driver_sql("""
    CREATE FUNCTION require_initial_acquisition() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.status<>'IMPORTING' OR NEW.observed<>0 OR NEW.accepted<>0 OR
         NEW.rejected<>0 OR NEW.error_count<>0 OR NEW.last_position<>0 OR
         NEW.last_event_time IS NOT NULL OR NEW.completed_at IS NOT NULL THEN
        RAISE EXCEPTION 'acquisitions must begin as empty importing attempts';
      END IF;
      RETURN NEW;
    END $$;
    CREATE TRIGGER acquisition_initial_guard BEFORE INSERT ON acquisition_runs
      FOR EACH ROW EXECUTE FUNCTION require_initial_acquisition();
    CREATE OR REPLACE FUNCTION guard_manifest_completion() RETURNS TRIGGER
    LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP='UPDATE' AND OLD.status='COMPLETE' THEN
        RAISE EXCEPTION 'completed dataset is immutable';
      END IF;
      IF NEW.status='COMPLETE' AND NOT EXISTS(
        SELECT 1 FROM acquisition_runs WHERE capture_id=NEW.selected_capture AND
          dataset_id=NEW.dataset_id AND status='COMPLETE') THEN
        RAISE EXCEPTION 'dataset completion requires a completed acquisition';
      END IF;
      RETURN NEW;
    END $$;
    DROP TRIGGER manifest_completion_guard ON dataset_manifests;
    CREATE TRIGGER manifest_completion_guard BEFORE INSERT OR UPDATE ON dataset_manifests
      FOR EACH ROW EXECUTE FUNCTION guard_manifest_completion();
    CREATE FUNCTION guard_source_context() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    DECLARE config JSONB; expected_received TIMESTAMPTZ;
    BEGIN
      SELECT d.configuration INTO config FROM acquisition_runs a
        JOIN dataset_manifests d USING(dataset_id) WHERE a.capture_id=NEW.capture_id;
      IF TG_TABLE_NAME='raw_observations' THEN
        IF NEW.provider<>config->>'provider' OR NEW.source_format<>
          (CASE WHEN config->>'source_type'='historical_csv' THEN 'csv' ELSE 'json' END) THEN
          RAISE EXCEPTION 'raw observation does not match acquisition configuration';
        END IF;
      ELSE
        SELECT COALESCE(received_at_utc,(config->>'received_at_utc')::timestamptz)
          INTO expected_received FROM raw_observations
          WHERE capture_id=NEW.capture_id AND source_position=NEW.source_position;
        IF NEW.provider_symbol<>config->>'provider_symbol' OR
           NEW.pip_size<>(config->>'pip_size')::integer OR
           NEW.source_type<>config->>'source_type' OR
           NEW.received_at_utc IS DISTINCT FROM expected_received THEN
          RAISE EXCEPTION 'market tick does not match acquisition configuration';
        END IF;
      END IF;
      RETURN NEW;
    END $$;
    CREATE TRIGGER raw_context_guard BEFORE INSERT ON raw_observations
      FOR EACH ROW EXECUTE FUNCTION guard_source_context();
    CREATE TRIGGER tick_context_guard BEFORE INSERT ON market_ticks
      FOR EACH ROW EXECUTE FUNCTION guard_source_context();
    CREATE FUNCTION forbid_evidence_removal() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
      RAISE EXCEPTION 'research evidence removal requires an explicit reviewed migration';
    END $$;
    CREATE TRIGGER acquisition_removal_guard BEFORE DELETE ON acquisition_runs
      FOR EACH ROW EXECUTE FUNCTION forbid_evidence_removal();
    CREATE TRIGGER manifest_removal_guard BEFORE DELETE ON dataset_manifests
      FOR EACH ROW EXECUTE FUNCTION forbid_evidence_removal();
    CREATE TRIGGER acquisition_truncate_guard BEFORE TRUNCATE ON acquisition_runs
      EXECUTE FUNCTION forbid_evidence_removal();
    CREATE TRIGGER manifest_truncate_guard BEFORE TRUNCATE ON dataset_manifests
      EXECUTE FUNCTION forbid_evidence_removal();
    CREATE TRIGGER raw_truncate_guard BEFORE TRUNCATE ON raw_observations
      EXECUTE FUNCTION forbid_evidence_removal();
    CREATE TRIGGER tick_truncate_guard BEFORE TRUNCATE ON market_ticks
      EXECUTE FUNCTION forbid_evidence_removal();
    CREATE TRIGGER quality_truncate_guard BEFORE TRUNCATE ON data_quality_events
      EXECUTE FUNCTION forbid_evidence_removal();
    CREATE TRIGGER batch_truncate_guard BEFORE TRUNCATE ON ingestion_batches
      EXECUTE FUNCTION forbid_evidence_removal();
    """)


def downgrade() -> None:
    raise RuntimeError("evidence guards require an explicitly reviewed schema migration")
