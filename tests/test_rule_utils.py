from spark.rule_utils import get_alert_reason, is_amount_deviation


def test_alert_reason_both_conditions():
    assert get_alert_reason(10, True) == "High txn count and amount deviation"


def test_alert_reason_txn_only():
    assert get_alert_reason(10, False) == "High txn count"


def test_alert_reason_deviation_only():
    assert get_alert_reason(2, True) == "Amount deviation"


def test_alert_reason_none():
    assert get_alert_reason(2, False) is None


def test_amount_deviation_true():
    assert is_amount_deviation(100, 10, 130) is True


def test_amount_deviation_false():
    assert is_amount_deviation(100, 10, 115) is False


def test_amount_deviation_std_none():
    assert is_amount_deviation(100, None, 150) is False


def test_amount_deviation_std_zero():
    assert is_amount_deviation(100, 0, 150) is False