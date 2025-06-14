from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.utils.dates import days_ago

from generate_data import stream_data_to_kafka, create_topic


logger = logging.getLogger(__name__)
TOPIC_NAME = "cdr-data"
DATA_LOADING_RUNTIME = 2

default_args = {
    "owner":"austin",
    "depends_on_past":False,
    "email_on_failure":False,
    "retries":3,
    "retry_delay":timedelta(seconds=5),
    "start_date":days_ago(1),
}

dag = DAG(
    "cdr-data",
    default_args=default_args,
    description="Generate and produce logs",
    schedule_interval="@daily",
    catchup=False,
)

with dag as d:
    task_1 = PythonOperator(
        task_id="create_kafka_topic",
        python_callable=create_topic,
        op_args=[TOPIC_NAME]
    )

    task_2 = PythonOperator(
        task_id="stream_to_data",
        python_callable=stream_data_to_kafka,
        op_args=[DATA_LOADING_RUNTIME, TOPIC_NAME]
    )

    task_1 >> task_2