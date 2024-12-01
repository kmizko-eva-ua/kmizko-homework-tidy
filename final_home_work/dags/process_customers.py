from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='process_customers',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2022,9,1),
    catchup=False,
    tags=['bigquery', 'customers'],
) as dag:

    load_to_bronze = GCSToBigQueryOperator(
        task_id='load_raw_customers_to_bronze',
        bucket='rd-de-raw-bkt',
        source_objects="customers/*.csv",
        destination_project_dataset_table='de-07-kirill-mizko.bronze.customers',
        source_format='CSV',
        field_delimiter=",",
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[
            {"name": "Id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "RegistrationDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "State", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    create_silver_table = BigQueryCreateEmptyTableOperator(
        task_id='create_silver_table',
        dataset_id='silver',
        table_id='customers',
        schema_fields=[
            {"name": "client_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "registration_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        ],
        project_id="de-07-kirill-mizko",
    )

    into_customer_table = """
                    SELECT distinct 
                        CAST(Id AS INT64) AS client_id,
                        TRIM(FirstName) AS first_name,
                        TRIM(LastName) AS last_name,
                        LOWER(Email) AS email,
                        CAST(RegistrationDate AS DATE) AS registration_date,
                        TRIM(State) AS state
                    FROM `de-07-kirill-mizko.bronze.customers`
                """

    load_to_silver = BigQueryInsertJobOperator(
        task_id='load_to_silver',
        configuration={
            "query": {
                "query": into_customer_table,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "de-07-kirill-mizko",
                    "datasetId": "silver",
                    "tableId": "customers",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    load_to_bronze >> create_silver_table >> load_to_silver