from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fraud_rule_engine_dag",
    default_args=default_args,
    description="Run fraud rule engine on feature_store",
    start_date=datetime(2026, 3, 13),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    run_fraud_rule_engine = BashOperator(
        task_id="run_fraud_rule_engine",
        bash_command="""
        docker exec de2-spark-master /opt/spark/bin/spark-submit \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --packages org.postgresql:postgresql:42.7.10 \
        /opt/spark-apps/batch_rule_engine.py
        """
    )