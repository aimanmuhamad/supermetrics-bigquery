import datetime as dt
import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import supermetrics_to_bigquery as smtbq

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2020,1,2),
    'retries': 5,
}


def execution(**kwargs):
    smtbq.upload_supermetrics_csv_result_to_bigquery(
        "<supermetrics_url>",
        "<table-name>",
        kwargs['_start'],
        kwargs['_end']
    )


with DAG('<dag-id>', default_args=default_args, schedule_interval='0 6 * * *') as dag:
    start_date = '{{ yesterday_ds }}'
    end_date = '{{ yesterday_ds }}'
    supermetrics_to_bigquery = PythonOperator(
        task_id='<task-id>',
        python_callable=execution,
        op_kwargs={'_start':start_date, '_end':end_date}
    )