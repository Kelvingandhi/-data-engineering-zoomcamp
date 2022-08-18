from datetime import datetime
import os
import logging

from sqlalchemy import table

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_to_pg_script import ingest_callable

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow')

PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/pg_input/csvs/' + 'output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

def ingest_to_pg(str):
    return str + ' ingested'

local_workflow = DAG("LocalIngestionToPgDag",
                    schedule_interval = "0 6 2 * *",
                    start_date = datetime(2021,1,1))


with local_workflow:

    curl_dataset_task = BashOperator(
                task_id = 'curl_dataset',
                bash_command = f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_to_pg_task = PythonOperator(
                task_id = 'ingest_pg',
                python_callable = ingest_callable,
                op_kwargs = dict(user=PG_USER,
                                 password=PG_PASSWORD,
                                 host=PG_HOST,
                                 port=PG_PORT,
                                 db=PG_DATABASE,
                                 table_name=TABLE_NAME_TEMPLATE,
                                 csv_name=OUTPUT_FILE_TEMPLATE)
    )

    curl_dataset_task >> ingest_to_pg_task