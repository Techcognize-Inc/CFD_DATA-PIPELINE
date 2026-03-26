import json
import time
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaProducer

TOPIC = "raw.card.transactions"
BOOTSTRAP_SERVERS = "localhost:9092"
CSV_PATH = "data/creditcard.csv"
BASE_TIME = datetime(2026, 3, 25, 19, 0, 0)


def build_transaction(row, idx):
    tx_time = BASE_TIME + timedelta(seconds=idx * 5)

    if idx < 20:
        card_id = "card_test_999"
    else:
        card_id = f"card_{idx % 5000}"

    if 5 <= idx < 10:
        amount = 5000.0
    else:
        amount = float(row["Amount"])

    return {
        "tx_id": f"tx_{idx}",
        "card_id": card_id,
        "merchant_id": f"merchant_{idx % 20}",
        "amount": amount,
        "tx_time": tx_time.isoformat(),
        "fraud_label": int(row["Class"])
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    df = pd.read_csv(CSV_PATH)

    for idx, row in df.head(50).iterrows():
        payload = build_transaction(row, idx)
        producer.send(TOPIC, value=payload)
        print(f"Sent: {payload}")
        time.sleep(0.2)

    producer.flush()
    print("Completed replay.")


if __name__ == "__main__":
    main()