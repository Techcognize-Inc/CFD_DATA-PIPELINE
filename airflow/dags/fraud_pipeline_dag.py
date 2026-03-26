from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fraud_pipeline_dag",
    default_args=default_args,
    description="Monitor fraud pipeline and generate fraud alerts",
    start_date=datetime(2026, 3, 25),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["fraud", "streaming"],
) as dag:

    kafka_topic_check = BashOperator(
        task_id="kafka_topic_check",
        bash_command="""
        docker exec de2-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep raw.card.transactions
        """
    )

    feature_store_check = BashOperator(
        task_id="feature_store_check",
        bash_command='''
        docker exec de2-postgres psql -U frauduser -d bankingdb -c "select count(*) from fraud.feature_store;"
        '''
    )

    generate_fraud_alerts = BashOperator(
        task_id="generate_fraud_alerts",
        bash_command="""
        docker exec de2-postgres psql -U frauduser -d bankingdb -f /opt/sql/generate_alerts.sql
        """
    )

    fraud_alerts_check = BashOperator(
        task_id="fraud_alerts_check",
        bash_command='''
        docker exec de2-postgres psql -U frauduser -d bankingdb -c "select * from fraud.fraud_alerts order by created_at desc limit 10;"
        '''
    )

    kafka_topic_check >> feature_store_check >> generate_fraud_alerts >> fraud_alerts_check