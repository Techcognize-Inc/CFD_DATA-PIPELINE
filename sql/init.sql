CREATE SCHEMA IF NOT EXISTS fraud;

CREATE TABLE IF NOT EXISTS fraud.feature_store (
  card_id TEXT NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  txn_count_10min INT,
  avg_amount_10min DOUBLE PRECISION,
  amount_deviation_flag BOOLEAN,
  PRIMARY KEY (card_id, window_start, window_end)
);

CREATE TABLE IF NOT EXISTS fraud.feature_store_stage (
  card_id TEXT NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  txn_count_10min INT,
  avg_amount_10min DOUBLE PRECISION,
  amount_deviation_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS fraud.fraud_rules (
  rule_id SERIAL PRIMARY KEY,
  rule_name TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  txn_count_threshold INT,
  deviation_flag_required BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS fraud.fraud_alerts (
  alert_id SERIAL PRIMARY KEY,
  card_id TEXT,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  alert_reason TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_card_time
ON fraud.fraud_alerts (card_id, created_at DESC);

CREATE OR REPLACE FUNCTION fraud.upsert_feature_store()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO fraud.feature_store (
    card_id,
    window_start,
    window_end,
    txn_count_10min,
    avg_amount_10min,
    amount_deviation_flag
  )
  SELECT
    s.card_id,
    s.window_start,
    s.window_end,
    s.txn_count_10min,
    s.avg_amount_10min,
    s.amount_deviation_flag
  FROM fraud.feature_store_stage s
  ON CONFLICT (card_id, window_start, window_end)
  DO UPDATE SET
    txn_count_10min = EXCLUDED.txn_count_10min,
    avg_amount_10min = EXCLUDED.avg_amount_10min,
    amount_deviation_flag = EXCLUDED.amount_deviation_flag;

  TRUNCATE TABLE fraud.feature_store_stage;
END;
$$;