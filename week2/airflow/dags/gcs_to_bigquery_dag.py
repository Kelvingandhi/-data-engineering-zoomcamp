from datetime import datetime
import os
import logging
import pyarrow.csv as csv
import pyarrow.parquet as pq
from sqlalchemy import table

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow')

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')

BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET','trips_data_all')

DATASET_PREFIX = 'tripdata'
INPUT_FILETYPE = 'parquet'
COLOR_RANGE = {'green_taxi':'lpep_pickup_datetime',
          'yellow_taxi':'tpep_pickup_datetime'}


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

gcs_bigquery_workflow =  DAG(
    dag_id = 'gcs_2_bq_dag',
    schedule_interval = "@daily",
    default_args = default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de']
)

with gcs_bigquery_workflow:

    for color, ds_column in COLOR_RANGE.items():

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                task_id = f"bq_{color}_{DATASET_PREFIX}_external_table_task",
                table_resource={
                    "tableReference" : {
                        "projectId" : PROJECT_ID,
                        "datasetId" : BIGQUERY_DATASET,
                        "tableId" : f"{color}_{DATASET_PREFIX}_external_table"
                    },
                    "externalDataConfiguration": {
                        "sourceFormat" : f"{INPUT_FILETYPE.upper()}",
                        "sourceUris": [f"gs://{BUCKET}/{color}/*"]
                    }
                }
            )

        CREATE_BQ_PART_TBL_QUERY = (
                            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{color}_{DATASET_PREFIX}_partitioned_table \
                            PARTITION BY DATE({ds_column}) \
                            AS \
                            SELECT * FROM {BIGQUERY_DATASET}.{color}_{DATASET_PREFIX}_external_table;"
                            )
        
        bigquery_partitioned_table_task = BigQueryInsertJobOperator(
            task_id = f"bq_{color}_{DATASET_PREFIX}_partitioned_table_task",
            configuration = {
                "query" : {
                    "query" : CREATE_BQ_PART_TBL_QUERY,
                    "useLegacySql" : False
                }
            }
        )


        bigquery_external_table_task >> bigquery_partitioned_table_task

