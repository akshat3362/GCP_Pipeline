from airflow import DAG
from airflow.utils.dates import datetime
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

default_args = {
'owner': 'airflow',
'start_date': datetime.now(),
'retries':1,
'retry_delay':timedelta(minutes=5),
}

with DAG(
"First_dag",
default_args = default_args,
description = "Dataflow job to insert data from GCS into BQ",
schedule_interval='*/5 * * * *', 
catchup=False,  # Disable catchup to prevent old job executions
max_active_runs=1,
) as dag:
    start = DummyOperator(
        task_id = "Start of the Pipeline",
    )
    run_dataflow_job = DataflowCreatePythonJobOperator(
        task_id = 'Dataflow Job',
        py_file = 'gs://practicegcp69/Dataflow_jobs/Orders_Variant_load.py',
        job_name = 'Orders_load_variant',
        options = {
            'project' : 'keen-dolphin-436707-b9',
            'staging_location' : 'gs://practicegcp69/Staging/',
            'temp_location' : 'gs://practicegcp69/Temp/',
            'region' : 'us-central1',
            'runner' : 'DataflowRunner'
        },
        gcp_conn_id='google_cloud_default',
    )
    stored_procedure_query = """
    CALL `keen-dolphin-436707-b9.EDM_CONFIRMED_PRD.SP_LOAD_ORDERS`();
    """
    run_stored_procedure = BigQueryInsertJobOperator(
        task_id='run_stored_procedure_task',
        configuration={
            "query": {
                "query": stored_procedure_query,
                "useLegacySql": False  # We are using Standard SQL
            }
        },
        location='US',  # Specify the location of your BigQuery dataset
    )

    # Task to move the file from one GCS location to another
    move_file_task = GCSToGCSOperator(
        task_id='move_file',
        source_bucket='practicegcp69',  # Replace with your source bucket name
        source_object='https://storage.cloud.google.com/practicegcp69/Json_input/*.json',  # Path to the file in the source bucket
        destination_bucket='practicegcp69',  # Replace with your destination bucket name
        destination_object='https://storage.cloud.google.com/practicegcp69/Archive/',  # Destination path for the file
        move_object=True  # Set to True to move (delete the source after copy)
    )

    end  = DummyOperator(
        task_id ="End of Pipeline"
    ) 

    start >> run_dataflow_job >> stored_procedure_query >> move_file_task