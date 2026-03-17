from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import socket

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def check_kafka():
    s = socket.socket()
    s.settimeout(5)
    s.connect(("de2-kafka", 29092))
    s.close()

def check_postgres():
    conn = psycopg2.connect(
        host="de2-postgres",
        port=5432,
        database="bankingdb",
        user="banking",
        password="password"
    )
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    cur.fetchone()
    cur.close()
    conn.close()

def check_feature_store():
    conn = psycopg2.connect(
        host="de2-postgres",
        port=5432,
        database="bankingdb",
        user="banking",
        password="password"
    )
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM fraud.feature_store;")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    if count == 0:
        raise ValueError("feature_store has no rows")

with DAG(
    dag_id="fraud_streaming_monitor_dag",
    default_args=default_args,
    description="Monitor fraud streaming pipeline",
    start_date=datetime(2026, 3, 13),
    schedule_interval="*/1 * * * *",
    catchup=False,
) as dag:

    kafka_health_check = PythonOperator(
        task_id="kafka_health_check",
        python_callable=check_kafka,
    )

    postgres_health_check = PythonOperator(
        task_id="postgres_health_check",
        python_callable=check_postgres,
    )

    feature_store_check = PythonOperator(
        task_id="feature_store_check",
        python_callable=check_feature_store,
    )

    kafka_health_check >> postgres_health_check >> feature_store_check