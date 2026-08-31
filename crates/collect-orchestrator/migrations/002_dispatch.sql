-- Nomad dispatch tracking: dispatched is an active lease like processing
ALTER TABLE parse_queue ADD COLUMN IF NOT EXISTS dispatched_at TIMESTAMPTZ;
ALTER TABLE parse_queue ADD COLUMN IF NOT EXISTS nomad_job_id TEXT;
ALTER TABLE parse_queue ADD COLUMN IF NOT EXISTS nomad_alloc_id TEXT;

-- Expand status check to include dispatched
ALTER TABLE parse_queue DROP CONSTRAINT IF EXISTS parse_queue_status_check;
ALTER TABLE parse_queue DROP CONSTRAINT IF EXISTS parse_queue_parser_check;
ALTER TABLE parse_queue ADD CONSTRAINT parse_queue_status_check CHECK (status IN ('pending','processing','failed','dead_letter','dispatched'));
ALTER TABLE parse_queue ADD CONSTRAINT parse_queue_parser_check CHECK (parser IN ('ais-parse','aisstream-parse'));

CREATE INDEX IF NOT EXISTS idx_parse_queue_dispatched_reclaim ON parse_queue (dispatched_at) WHERE status='dispatched';
CREATE INDEX IF NOT EXISTS idx_parse_queue_status_dispatched ON parse_queue (status) WHERE status='dispatched';
