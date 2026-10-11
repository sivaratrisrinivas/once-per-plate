-- Content-addressed bytes of every export that arrived.
CREATE TABLE IF NOT EXISTS blobs (
  sha256 text PRIMARY KEY,
  content bytea NOT NULL,
  size int NOT NULL
);

-- One row per QC run. run_key is the idempotency key: sha256 + instrument + plate.
CREATE TABLE IF NOT EXISTS runs (
  id bigserial PRIMARY KEY,
  run_key text NOT NULL UNIQUE,
  sha256 text NOT NULL REFERENCES blobs(sha256),
  plate_id text NOT NULL,
  instrument_id text NOT NULL,
  exported_at timestamptz,
  version int NOT NULL,
  supersedes_run_id bigint REFERENCES runs(id),
  state text NOT NULL CHECK (state IN ('RECEIVED','QUEUED','RUNNING','PROPOSED','APPROVED',
                                       'WRITTEN','REJECTED','SUPERSEDED','PARKED')),
  attempts int NOT NULL DEFAULT 0,
  lease_owner text,
  lease_until timestamptz,
  published_at timestamptz,
  result jsonb,
  summary jsonb,
  decided_by text,
  decided_at timestamptz,
  note text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (plate_id, version)
);

-- Every file arrival, including duplicates, with what the ledger decided.
CREATE TABLE IF NOT EXISTS arrivals (
  id bigserial PRIMARY KEY,
  filename text NOT NULL,
  source text NOT NULL,
  sha256 text NOT NULL,
  size int NOT NULL,
  outcome text NOT NULL CHECK (outcome IN ('NEW_RUN','NEW_VERSION','DUPLICATE','STALE')),
  run_id bigint NOT NULL REFERENCES runs(id),
  arrived_at timestamptz NOT NULL DEFAULT now()
);

-- Append-only audit log. Every state change carries to_state, so state can be rebuilt from it.
CREATE TABLE IF NOT EXISTS events (
  id bigserial PRIMARY KEY,
  run_id bigint NOT NULL REFERENCES runs(id),
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  actor text NOT NULL,
  kind text NOT NULL,
  to_state text,
  detail text NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS events_run ON events(run_id, id);

-- Stand-in for a Labsheet. One row per well per approved run; a retried write is a no-op.
CREATE TABLE IF NOT EXISTS labsheet_rows (
  run_id bigint NOT NULL REFERENCES runs(id),
  well text NOT NULL,
  plate_id text NOT NULL,
  sample_id text NOT NULL,
  condition text NOT NULL,
  spot_count text NOT NULL,
  verdict text NOT NULL,
  written_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, well)
);
