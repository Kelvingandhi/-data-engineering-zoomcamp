from datetime import datetime
import os
import logging
import pyarrow.csv as csv
import pyarrow.parquet as pq
from sqlalchemy import table

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow')

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = 'FHV_trips_data'

FHV_URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
FHV_URL_TEMPLATE = FHV_URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/pg_input/csvs/' + 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/pg_input/csvs/' + 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_GCS_PATH_TEMPLATE = "raw/fhv_tripdata_{{ execution_date.strftime(\'%Y_%m\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def format_csv_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Only accept .csv file")
        return
    table = csv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv','.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

dag_workflow = DAG("FHV_Data_Ingestion_Dag",
                    schedule_interval = "0 8 2 * *",
                    start_date = datetime(2019,2,1),
                    end_date = datetime(2019,3,1),
                    catchup=False,
                    #max_active_runs=3,
                    default_args=default_args,
                    tags=['dtc-de'],)

with dag_workflow:

    download_fhv_data_task = BashOperator(
        task_id = 'curl_fhv_data_task',
        bash_command = f'curl -sSL {FHV_URL_TEMPLATE} > {FHV_CSV_FILE_TEMPLATE}'
    )

    format_to_parquet_task = PythonOperator(
        task_id = 'format_to_parquet_task',
        python_callable = format_csv_to_parquet,
        op_kwargs = {"src_file":FHV_CSV_FILE_TEMPLATE}
    )

    upload_to_gcs_task = PythonOperator(
        task_id = 'upload_task',
        python_callable = upload_to_gcs,
        op_kwargs = {
                "bucket" : BUCKET,
                "object_name" : FHV_GCS_PATH_TEMPLATE,
                "local_file" : FHV_PARQUET_FILE_TEMPLATE
                }
    )

    clean_up_local_task = BashOperator(
        task_id = 'clean_up_local_files_task',
        bash_command = f'rm {FHV_CSV_FILE_TEMPLATE} {FHV_PARQUET_FILE_TEMPLATE}'
    )

    download_fhv_data_task >> format_to_parquet_task >> upload_to_gcs_task >> clean_up_local_task