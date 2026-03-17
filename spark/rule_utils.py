def get_alert_reason(txn_count_10min, amount_deviation_flag):
    if txn_count_10min > 5 and amount_deviation_flag:
        return "High txn count and amount deviation"
    if txn_count_10min > 5:
        return "High txn count"
    if amount_deviation_flag:
        return "Amount deviation"
    return None


def is_amount_deviation(avg_amount, std_amount, max_amount):
    if std_amount is None or std_amount <= 0:
        return False
    return max_amount > (avg_amount + 3 * std_amount)