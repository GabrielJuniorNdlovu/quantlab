# Feature 04 ingestion benchmark status

The benchmark harness is implemented in [`ingestion.py`](ingestion.py), but no scale results
are recorded for this branch. It requires an isolated PostgreSQL/TimescaleDB instance and
`QUANTLAB_TEST_DATABASE_URL`. Docker Desktop is unavailable in the current environment, and
there is no local PostgreSQL server. The required 100,000 and 1,000,000 record historical,
streaming, recovery, replay, and Parquet measurements therefore remain unrun.

Do not use the provisional batch or queue defaults as measured capacity guidance. After the
database is available, run `python -m benchmarks.ingestion`, retain the generated JSON and
profile, then replace this status with the actual environment, results, and limitations.
