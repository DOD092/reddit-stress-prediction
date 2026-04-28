import sys, os
from datetime import datetime, timedelta

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from airflow import DAG
from airflow.operators.python import PythonOperator

from _kafka.produce import produce_praw
from _spark.stream import structured_stream

default_args = {
    'owner': 'DAT',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='online_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['reddit_predict', 'online', 'streaming']
) as dag:

    crawling = PythonOperator(
        task_id='crawl_data',
        python_callable=produce_praw,
    )

    streaming = PythonOperator(
        task_id='stream_data',
        python_callable=structured_stream,
        execution_timeout=None,
    )

    crawling >> streaming