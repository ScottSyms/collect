CREATE TABLE IF NOT EXISTS parse_queue (
  s3_bucket      TEXT NOT NULL,
  s3_key         TEXT PRIMARY KEY,
  source         TEXT NOT NULL,
  parser         TEXT NOT NULL CHECK (parser IN ('ais-parse','aisstream-parse')),
  status         TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','failed','dead_letter')),
  attempts       INT  NOT NULL DEFAULT 0,
  max_attempts   INT  NOT NULL DEFAULT 5,
  last_error     TEXT,
  next_retry_at  TIMESTAMPTZ,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_at      TIMESTAMPTZ,
  locked_by      TEXT
);
CREATE INDEX IF NOT EXISTS idx_parse_queue_status_retry ON parse_queue (status, next_retry_at) WHERE status IN ('pending','failed');
CREATE INDEX IF NOT EXISTS idx_parse_queue_source ON parse_queue (source);

CREATE TABLE IF NOT EXISTS parse_history (
  s3_bucket      TEXT NOT NULL,
  s3_key         TEXT PRIMARY KEY,
  source         TEXT NOT NULL,
  parser         TEXT NOT NULL,
  attempts       INT  NOT NULL,
  duration_ms    INT,
  rows_in        BIGINT,
  positions_out  BIGINT,
  statics_out    BIGINT,
  meteo_out      BIGINT,
  binary_out     BIGINT,
  atons_out      BIGINT,
  other_out      BIGINT,
  incomplete     BIGINT,
  unparsed       BIGINT,
  deduped        BIGINT,
  created_at     TIMESTAMPTZ NOT NULL,
  completed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
