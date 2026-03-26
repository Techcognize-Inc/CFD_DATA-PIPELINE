CREATE SCHEMA IF NOT EXISTS fraud;

CREATE TABLE IF NOT EXISTS fraud.feature_store (
  card_id TEXT NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  txn_count_10min INT,
  avg_amount_10min DOUBLE PRECISION,
  amount_deviation_flag BOOLEAN,
  stddev_amount_10min DOUBLE PRECISION,
  max_amount_10min DOUBLE PRECISION,
  min_amount_10min DOUBLE PRECISION,
  batch_id BIGINT,
  PRIMARY KEY (card_id, window_start, window_end)
);

CREATE TABLE IF NOT EXISTS fraud.feature_store_stage (
  card_id TEXT NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  txn_count_10min INT,
  avg_amount_10min DOUBLE PRECISION,
  amount_deviation_flag BOOLEAN,
  stddev_amount_10min DOUBLE PRECISION,
  max_amount_10min DOUBLE PRECISION,
  min_amount_10min DOUBLE PRECISION,
  batch_id BIGINT
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