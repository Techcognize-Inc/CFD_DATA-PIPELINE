INSERT INTO fraud.fraud_alerts (
    card_id,
    window_start,
    window_end,
    alert_reason,
    created_at
)
SELECT
    fs.card_id,
    fs.window_start,
    fs.window_end,
    CASE
        WHEN fs.txn_count_10min >= 10 AND fs.amount_deviation_flag = TRUE
            THEN 'High txn count and amount deviation'
        WHEN fs.txn_count_10min >= 10
            THEN 'High txn count'
        WHEN fs.amount_deviation_flag = TRUE
            THEN 'Amount deviation'
        ELSE 'Rule matched'
    END AS alert_reason,
    NOW()
FROM fraud.feature_store fs
WHERE
    fs.txn_count_10min >= 10
    OR fs.amount_deviation_flag = TRUE;