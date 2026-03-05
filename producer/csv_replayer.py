import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "raw.card.transactions"
CSV_PATH = "data/creditcard.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def main():

    df = pd.read_csv(CSV_PATH)
    base_time = datetime.now()

    for i, row in df.iterrows():

        tx = {
            "tx_id": f"tx_{i}",
            "card_id": f"card_{i % 1000}",
            "merchant_id": f"merchant_{i % 200}",
            "amount": float(row["Amount"]),
            "tx_time": (base_time + timedelta(seconds=int(row["Time"]))).isoformat(),
            "fraud_label": int(row["Class"])
        }

        producer.send(TOPIC, tx)

        if i % 1000 == 0:
            producer.flush()
            print(f"Sent {i} transactions")

        time.sleep(0.01)

    producer.flush()

if __name__ == "__main__":
    main()