import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.bash import BashOperator

# Constants configuration
GCS_BUCKET = os.environ.get("GCS_BUCKET", "housing_ml_bucket")
GCS_STAGING_OBJECT = "staging/housing.csv"
LOCAL_FILE_PATH = "/opt/data/housing.csv"

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

@dag(
    dag_id="housing_etl_pipeline",
    default_args=default_args,
    schedule=None, # Set None for this project, consider setting as @daily for future
    start_date=datetime(2025, 1, 1),
    catchup=False, # Do not run for the past dates
    tags=['etl', 'spark', 'ml'],
)
def housing_etl_pipeline():
    # Task 1: Upload local .csv file to GCS staging folder
    upload_to_staging = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_staging",
        src=LOCAL_FILE_PATH,
        dst=GCS_STAGING_OBJECT,
        bucket=GCS_BUCKET,
        gcp_conn_id="google_cloud_default",
        mime_type="text/csv",
    )
    
    # Task 2: Writes batches from staging to raw
    staging_to_raw_batches = BashOperator(
        task_id="staging_to_raw_batches",
        bash_command="""
            spark-submit \
            --master local[*] \
            /opt/airflow/spark_jobs/staging_to_raw_batches.py
        """,
    )

    # Task 3: Train ML model and save the pipeline
    train_ml_model = BashOperator(
        task_id="train_ml_model",
        bash_command="""
            spark-submit \
            --master local[*] \
            /opt/airflow/spark_jobs/ml_model.py
        """
    )

    upload_to_staging >> staging_to_raw_batches >> train_ml_model

housing_etl_pipeline()