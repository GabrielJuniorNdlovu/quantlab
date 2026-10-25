"""Initial PostgreSQL/Timescale schema, owned by the additive Alembic revision."""

SCHEMA = """
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE dataset_manifests (
 dataset_id UUID PRIMARY KEY,
 source_sha256 TEXT NOT NULL CHECK(source_sha256 ~ '^[0-9a-f]{64}$'),
 source_bytes BIGINT NOT NULL CHECK(source_bytes>=0),
 source_path TEXT NOT NULL,
 source_id TEXT NOT NULL,
 configuration JSONB NOT NULL,
 configuration_sha256 TEXT NOT NULL CHECK(configuration_sha256 ~ '^[0-9a-f]{64}$'),
 status TEXT NOT NULL CHECK(status IN ('IMPORTING','COMPLETE','FAILED','INTERRUPTED')),
 selected_capture UUID,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 completed_at TIMESTAMPTZ,
 UNIQUE(source_sha256,configuration_sha256,source_id)
);
CREATE TABLE acquisition_runs (
 capture_id UUID PRIMARY KEY,
 dataset_id UUID NOT NULL REFERENCES dataset_manifests(dataset_id),
 instrument_id TEXT NOT NULL CHECK(instrument_id='BOOM_1000'),
 status TEXT NOT NULL CHECK(status IN ('IMPORTING','COMPLETE','FAILED','INTERRUPTED')),
 started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 completed_at TIMESTAMPTZ,
 observed BIGINT NOT NULL DEFAULT 0 CHECK(observed>=0),
 accepted BIGINT NOT NULL DEFAULT 0 CHECK(accepted>=0),
 rejected BIGINT NOT NULL DEFAULT 0 CHECK(rejected>=0),
 error_count BIGINT NOT NULL DEFAULT 0 CHECK(error_count>=0),
 last_position BIGINT NOT NULL DEFAULT 0 CHECK(last_position>=0),
 last_event_time TIMESTAMPTZ,
 CHECK(observed=accepted+rejected),
 UNIQUE(dataset_id,capture_id)
);
ALTER TABLE dataset_manifests ADD CONSTRAINT selected_attempt_fk
 FOREIGN KEY(dataset_id,selected_capture) REFERENCES acquisition_runs(dataset_id,capture_id);
CREATE TABLE raw_observations (
 capture_id UUID NOT NULL REFERENCES acquisition_runs(capture_id),
 source_position BIGINT NOT NULL CHECK(source_position>0),
 provider TEXT NOT NULL,
 source_format TEXT NOT NULL CHECK(source_format IN ('csv','json')),
 raw_payload BYTEA NOT NULL CHECK(octet_length(raw_payload) BETWEEN 1 AND 65536),
 payload_sha256 TEXT NOT NULL CHECK(payload_sha256=encode(digest(raw_payload,'sha256'),'hex')),
 received_at_utc TIMESTAMPTZ,
 source_metadata JSONB NOT NULL,
 accepted_at TIMESTAMPTZ,
 PRIMARY KEY(capture_id,source_position),
 UNIQUE(capture_id,source_position,accepted_at)
);
CREATE TABLE market_ticks (
 instrument_id TEXT NOT NULL CHECK(instrument_id='BOOM_1000'),
 provider_symbol TEXT NOT NULL CHECK(length(provider_symbol) BETWEEN 1 AND 256),
 event_time_utc TIMESTAMPTZ NOT NULL CHECK(isfinite(event_time_utc)),
 price NUMERIC NOT NULL CHECK(price>0 AND price::text NOT IN ('NaN','Infinity','-Infinity')),
 price_text TEXT NOT NULL CHECK(length(price_text)<=2048 AND price_text::numeric=price),
 pip_size INTEGER NOT NULL CHECK(pip_size BETWEEN 0 AND 18),
 source_type TEXT NOT NULL CHECK(source_type IN ('historical_csv','historical_json','websocket')),
 capture_id UUID NOT NULL,
 source_position BIGINT NOT NULL CHECK(source_position>0),
 received_at_utc TIMESTAMPTZ,
 provider_tick_id TEXT CHECK(provider_tick_id IS NULL OR
  length(provider_tick_id) BETWEEN 1 AND 256),
 CHECK(scale(price)<=pip_size AND scale(price_text::numeric)<=pip_size),
 PRIMARY KEY(event_time_utc,capture_id,source_position),
 FOREIGN KEY(capture_id,source_position,event_time_utc)
 REFERENCES raw_observations(capture_id,source_position,accepted_at)
);
SELECT create_hypertable('market_ticks',by_range('event_time_utc',INTERVAL '7 days'));
CREATE INDEX market_ticks_dataset_order ON market_ticks(capture_id,event_time_utc,source_position);
CREATE TABLE data_quality_events (
 event_id UUID PRIMARY KEY,
 capture_id UUID NOT NULL REFERENCES acquisition_runs(capture_id),
 source_position BIGINT CHECK(source_position IS NULL OR source_position>0),
 error_code TEXT NOT NULL,
 severity TEXT NOT NULL CHECK(severity IN ('warning','error','fatal')),
 description TEXT NOT NULL CHECK(length(description)<=512),
 field_name TEXT CHECK(field_name IS NULL OR length(field_name)<=64),
 observed_value TEXT CHECK(observed_value IS NULL OR length(observed_value)<=128),
 detected_at_utc TIMESTAMPTZ
);
CREATE TABLE ingestion_batches (
 capture_id UUID NOT NULL REFERENCES acquisition_runs(capture_id),
 batch_number BIGINT NOT NULL CHECK(batch_number>0),
 digest TEXT NOT NULL CHECK(digest ~ '^[0-9a-f]{64}$'),
 first_position BIGINT NOT NULL,
 last_position BIGINT NOT NULL CHECK(last_position>=first_position),
 observation_count BIGINT NOT NULL CHECK(observation_count>0),
 committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(capture_id,batch_number)
);
CREATE FUNCTION guard_append_only() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP <> 'INSERT' THEN
  RAISE EXCEPTION 'research evidence is append-only';
 END IF;
 IF (SELECT status FROM acquisition_runs WHERE capture_id=NEW.capture_id) <> 'IMPORTING' THEN
  RAISE EXCEPTION 'research writes require an importing acquisition';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER raw_append_guard BEFORE INSERT OR UPDATE OR DELETE ON raw_observations
 FOR EACH ROW EXECUTE FUNCTION guard_append_only();
CREATE TRIGGER tick_append_guard BEFORE INSERT OR UPDATE OR DELETE ON market_ticks
 FOR EACH ROW EXECUTE FUNCTION guard_append_only();
CREATE TRIGGER quality_append_guard BEFORE INSERT OR UPDATE OR DELETE ON data_quality_events
 FOR EACH ROW EXECUTE FUNCTION guard_append_only();
CREATE TRIGGER batch_append_guard BEFORE INSERT OR UPDATE OR DELETE ON ingestion_batches
 FOR EACH ROW EXECUTE FUNCTION guard_append_only();
CREATE FUNCTION guard_acquisition_completion() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.status IN ('COMPLETE','FAILED','INTERRUPTED') THEN
  RAISE EXCEPTION 'terminal acquisition is immutable';
 END IF;
 IF NEW.status='COMPLETE' THEN
  IF NEW.observed=0 OR NEW.rejected<>0 OR NEW.error_count<>0 OR
   NEW.observed<>(SELECT count(*) FROM raw_observations WHERE capture_id=NEW.capture_id) OR
   NEW.accepted<>(SELECT count(*) FROM market_ticks WHERE capture_id=NEW.capture_id) OR
   EXISTS(SELECT 1 FROM data_quality_events
    WHERE capture_id=NEW.capture_id AND severity<>'warning') THEN
   RAISE EXCEPTION 'acquisition completion requires reconciled error-free data';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER acquisition_completion_guard BEFORE UPDATE ON acquisition_runs
 FOR EACH ROW EXECUTE FUNCTION guard_acquisition_completion();
CREATE FUNCTION guard_manifest_completion() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.status='COMPLETE' THEN
  RAISE EXCEPTION 'completed dataset is immutable';
 END IF;
 IF NEW.status='COMPLETE' AND NOT EXISTS(
  SELECT 1 FROM acquisition_runs WHERE capture_id=NEW.selected_capture AND
   dataset_id=NEW.dataset_id AND status='COMPLETE') THEN
  RAISE EXCEPTION 'dataset completion requires a completed acquisition';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER manifest_completion_guard BEFORE UPDATE ON dataset_manifests
 FOR EACH ROW EXECUTE FUNCTION guard_manifest_completion();
"""
