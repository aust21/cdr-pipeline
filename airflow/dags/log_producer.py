from datetime import timedelta, datetime

from confluent_kafka import Producer
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

from faker import Faker

logger = logging.getLogger(__name__)
fake = Faker()

default_args = {
    "owner":"austin",
    "depends_on_past":False,
    "email_on_failure":False,
    "retries":3,
    "retry_delay":timedelta(seconds=5)
}

def produce_logs(**context):
    """ Produce log entries to kafka """


dag = DAG(
    "log_generation",
    default_args=default_args,
    description="Generate and produce logs",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 4, 24),
    catchup=False,
    tags=["logs", "kafka", "production"]
)

produce_tasks_logs = PythonOperator(
    task_id="generate_and_produce_logs",
    python_callable=produce_logs,
    dag=dag
)