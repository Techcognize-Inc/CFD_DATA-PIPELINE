from datetime import datetime
from producer.csv_replayer import build_transaction


def test_build_transaction():
    row = {
        "Amount": 250.75,
        "Time": 120,
        "Class": 1
    }
    base_time = datetime(2026, 3, 17, 10, 0, 0)

    tx = build_transaction(5, row, base_time)

    assert tx["tx_id"] == "tx_5"
    assert tx["card_id"] == "card_5"
    assert tx["merchant_id"] == "merchant_5"
    assert tx["amount"] == 250.75
    assert tx["fraud_label"] == 1
    assert tx["tx_time"] == "2026-03-17T10:02:00"


def test_build_transaction_zero_amount():
    row = {
        "Amount": 0,
        "Time": 0,
        "Class": 0
    }
    base_time = datetime(2026, 3, 17, 10, 0, 0)

    tx = build_transaction(0, row, base_time)

    assert tx["tx_id"] == "tx_0"
    assert tx["card_id"] == "card_0"
    assert tx["merchant_id"] == "merchant_0"
    assert tx["amount"] == 0.0
    assert tx["fraud_label"] == 0
    assert tx["tx_time"] == "2026-03-17T10:00:00"