from airflow import DAG
from datetime import datetime
import os
import sys

from airflow.operators.python import PythonOperator
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.reddit_pipeline import reddit_pipeline

default_args = {
    'owner': 'Krishna Teja Samudrala',
    #'depends_on_past': False,
    'start_date': datetime(2025, 3, 23),
    #'email': ['krishnatejasamudrala.work@gmail.com']
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

file_postfix = datetime.now().strftime('%Y%m%d')

dag = DAG(
    dag_id = 'etl_reddit_pipeline',
    default_args=default_args,
    description='Reddit Data Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit','etl']
)

extract = PythonOperator(
    task_id='extract',
        python_callable = reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)