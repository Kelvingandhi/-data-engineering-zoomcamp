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


#Defining DAG Method to handle yellow and green taxi data flow
def trip_data_to_gcs_flow(dag,
    url_template,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template):

    with dag:
        download_trip_data_task = BashOperator(
            task_id = 'curl_trip_data_task',
            bash_command = f'curl -sSL {url_template} > {local_csv_path_template}'
        )

        format_to_parquet_task = PythonOperator(
            task_id = 'format_to_parquet_task',
            python_callable = format_csv_to_parquet,
            op_kwargs = {"src_file":local_csv_path_template}
        )

        upload_to_gcs_task = PythonOperator(
            task_id = 'upload_task',
            python_callable = upload_to_gcs,
            op_kwargs = {
                "bucket":BUCKET,
                "object_name":gcs_path_template,
                "local_file":local_parquet_path_template
            }
        )

        clean_up_local_task = BashOperator(
            task_id = 'clean_up_task',
            bash_command = f'rm {local_csv_path_template} {local_parquet_path_template}'
        )

        download_trip_data_task >> format_to_parquet_task >> upload_to_gcs_task >> clean_up_local_task

    print("Done with DAG Processing")

# Yellow_Taxi data flow
YELLOW_TAXI_URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
YELLOW_TAXI_URL_TEMPLATE = YELLOW_TAXI_URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/pg_input/csvs/' + 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/pg_input/csvs/' + 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_PATH_TEMPLATE = "yellow_taxi/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

yellow_taxi_data_dag = DAG(
    dag_id = 'yellow_taxi_data_dag',
    schedule_interval = "0 8 2 * *",
    start_date = datetime(2021,1,1),
    end_date = datetime(2021,3,1),
    catchup=True,
    #max_active_runs=3,
    default_args=default_args,
    tags=['dtc-de'],
)

# Passing to DAG Processing
trip_data_to_gcs_flow(yellow_taxi_data_dag,
                    YELLOW_TAXI_URL_TEMPLATE,
                    YELLOW_TAXI_CSV_FILE_TEMPLATE,
                    YELLOW_TAXI_PARQUET_FILE_TEMPLATE,
                    YELLOW_TAXI_GCS_PATH_TEMPLATE)


# Green_Taxi data flow
GREEN_TAXI_URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
GREEN_TAXI_URL_TEMPLATE = GREEN_TAXI_URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
GREEN_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/pg_input/csvs/' + 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
GREEN_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/pg_input/csvs/' + 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TAXI_GCS_PATH_TEMPLATE = "green_taxi/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

green_taxi_data_dag = DAG(
    dag_id = 'green_taxi_data_dag',
    schedule_interval = "0 8 2 * *",
    start_date = datetime(2021,1,1),
    end_date = datetime(2021,3,1),
    catchup=True,
    #max_active_runs=3,
    default_args=default_args,
    tags=['dtc-de'],
)

# Passing to DAG Processing
trip_data_to_gcs_flow(green_taxi_data_dag,
                    GREEN_TAXI_URL_TEMPLATE,
                    GREEN_TAXI_CSV_FILE_TEMPLATE,
                    GREEN_TAXI_PARQUET_FILE_TEMPLATE,
                    GREEN_TAXI_GCS_PATH_TEMPLATE)