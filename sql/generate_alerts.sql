INSERT INTO fraud.fraud_alerts (card_id, window_start, window_end, alert_reason)
SELECT
  fs.card_id,
  fs.window_start,
  fs.window_end,
  CASE
    WHEN fs.amount_deviation_flag THEN 'Amount deviation spike'
    WHEN fs.txn_count_10min >= 10 THEN 'High txn count in 10 min'
  END AS alert_reason
FROM fraud.feature_store fs
WHERE fs.amount_deviation_flag = TRUE
   OR fs.txn_count_10min >= 10;