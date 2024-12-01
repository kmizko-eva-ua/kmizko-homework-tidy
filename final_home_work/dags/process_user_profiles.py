from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='process_user_profiles',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2022, 9, 1),
    catchup=False,
    tags=['bigquery', 'user_profiles'],
) as dag:
    load_silver_user_profiles = GCSToBigQueryOperator(
        task_id='load_silver_user_profiles',
        bucket='rd-de-raw-bkt',
        source_objects=['user_profiles/user_profiles.json'],
        destination_project_dataset_table='de-07-kirill-mizko.silver.user_profiles',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
        schema_fields=[
            {"name": "email"       , "type": "STRING", "mode": "NULLABLE"},
            {"name": "full_name"   , "type": "STRING", "mode": "NULLABLE"},
            {"name": "state"       , "type": "STRING", "mode": "NULLABLE"},
            {"name": "birth_date"  , "type": "DATE"  , "mode": "NULLABLE"},
            {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    run_enrich_user_profiles_dag = TriggerDagRunOperator(
        task_id='run_enrich_user_profiles_dag',
        trigger_dag_id='enrich_user_profiles',
    )

    load_silver_user_profiles >> run_enrich_user_profiles_dag