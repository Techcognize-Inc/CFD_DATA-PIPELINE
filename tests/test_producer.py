from producer.csv_replayer import build_transaction


def test_build_transaction_basic_fields():
    row = {
        "Amount": 250.75,
        "Class": 1
    }

    result = build_transaction(row, 25)

    assert result["tx_id"] == "tx_25"
    assert result["card_id"] == "card_25"
    assert result["merchant_id"] == "merchant_5"
    assert result["amount"] == 250.75
    assert result["fraud_label"] == 1
    assert "tx_time" in result


def test_build_transaction_forced_test_card():
    row = {
        "Amount": 100.0,
        "Class": 0
    }

    result = build_transaction(row, 3)

    assert result["card_id"] == "card_test_999"
    assert result["tx_id"] == "tx_3"
    assert result["merchant_id"] == "merchant_3"
    assert result["amount"] == 100.0
    assert result["fraud_label"] == 0


def test_build_transaction_forced_high_amount():
    row = {
        "Amount": 99.99,
        "Class": 0
    }

    result = build_transaction(row, 7)

    assert result["card_id"] == "card_test_999"
    assert result["amount"] == 5000.0


def test_build_transaction_normal_amount_after_override_range():
    row = {
        "Amount": 45.50,
        "Class": 0
    }

    result = build_transaction(row, 15)

    assert result["card_id"] == "card_test_999"
    assert result["amount"] == 45.50


def test_build_transaction_normal_card_after_test_range():
    row = {
        "Amount": 60.25,
        "Class": 0
    }

    result = build_transaction(row, 22)

    assert result["card_id"] == "card_22"
    assert result["merchant_id"] == "merchant_2"
    assert result["amount"] == 60.25


def test_build_transaction_time_field_format():
    row = {
        "Amount": 10.0,
        "Class": 0
    }

    result = build_transaction(row, 1)

    assert isinstance(result["tx_time"], str)
    assert "T" in result["tx_time"]