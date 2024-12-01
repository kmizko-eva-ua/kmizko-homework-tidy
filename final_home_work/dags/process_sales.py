from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner'          : 'airflow',
    'depends_on_past': False,
    'retries'        : 1,
}

with DAG(
    dag_id='final_process_sales',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime(2022,9,1),
    tags=['final', 'sales'],
) as dag:

    load_bronze_sales = GCSToBigQueryOperator(
        task_id = 'load_bronze_sales',
        bucket = 'rd-de-raw-bkt',
        source_objects = "sales/*.csv",
        destination_project_dataset_table = 'de-07-kirill-mizko.bronze.sales',
        source_format = 'CSV',
        field_delimiter = ",",
        skip_leading_rows = 1,
        write_disposition = 'WRITE_TRUNCATE',
        schema_fields = [
            {"name": "CustomerId"  , "type": "STRING", "mode": "NULLABLE"},
            {"name": "PurchaseDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Product"     , "type": "STRING", "mode": "NULLABLE"},
            {"name": "Price"       , "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    ddl_silver_seles = """
    CREATE TABLE IF NOT EXISTS `de-07-kirill-mizko.silver.sales` 
    (
         client_id     INT64
        ,purchase_date DATE
        ,product_name STRING
        ,price        FLOAT64
    )
    PARTITION BY purchase_date
    """


    load_data_query = """
    SELECT
         CAST(CustomerId AS INT64) AS client_id
        ,CASE
             --YYYY-MM-DD
             WHEN REGEXP_CONTAINS(TRIM(PurchaseDate), r'\d{4}-\d{2}-\d{2}') THEN PARSE_DATE('%Y-%m-%d', TRIM(PurchaseDate))
             --YYYY-MMM-DD
             WHEN REGEXP_CONTAINS(TRIM(PurchaseDate), r'\d{4}-[a-zA-Z]{3}-\d{2}') THEN PARSE_DATE('%Y-%b-%d', TRIM(PurchaseDate))
             --YYYY/MM/DD
             WHEN REGEXP_CONTAINS(TRIM(PurchaseDate), r'\d{4}/\d{2}/\d{2}') THEN PARSE_DATE('%Y/%m/%d', TRIM(PurchaseDate))
             --YYYY-MM-D
             WHEN REGEXP_CONTAINS(TRIM(PurchaseDate), r'\d{4}-\d{2}-\d{1}') THEN PARSE_DATE('%Y-%m-%d', FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y-%m-%d', REPLACE(TRIM(PurchaseDate), '-', '-'))))
             ELSE NULL
         END AS purchase_date
        ,SAFE_CAST(Product AS STRING) AS product_name
        ,CAST(REPLACE(REPLACE(Price, '$', ''), 'USD', '') AS FLOAT64) AS price
    FROM bronze.sales;
    """

    create_silver_sales = BigQueryInsertJobOperator(
        task_id='create_silver_sales',
        configuration={
            "query": {
                "query": ddl_silver_seles,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    load_silver_sales = BigQueryInsertJobOperator(
        task_id='load_silver_sales',
        configuration={
            "query": {
                "query": load_data_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "de-07-kirill-mizko",
                    "datasetId": "silver",
                    "tableId": "sales",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location="US",
    )

    load_bronze_sales >> create_silver_sales >> load_silver_sales