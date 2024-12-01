from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='enrich_user_profiles',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2022, 9, 1),
    catchup=False,
    tags=['gold', 'enrichment'],
) as dag:

    gold_enrich_user_profiles = BigQueryInsertJobOperator(
        task_id='gold_enrich_user_profiles',
        configuration={
            "query": {
                "query": """
                    CREATE TABLE IF NOT EXISTS `de-07-kirill-mizko.gold.user_profiles_enriched` (
                        client_id INT64,
                        first_name STRING,
                        last_name STRING,
                        email STRING,
                        registration_date DATE,
                        state STRING,
                        phone_number STRING,
                        birth_date DATE
                    );
                    
                    
                    MERGE `de-07-kirill-mizko.gold.user_profiles_enriched` T
                    USING (
                        SELECT distinct 
                            c.client_id,
                            COALESCE(c.first_name, SPLIT(p.full_name, ' ')[OFFSET(0)]) AS first_name,
                            COALESCE(c.last_name, SPLIT(p.full_name, ' ')[OFFSET(1)]) AS last_name,
                            c.email,
                            c.registration_date,
                            COALESCE(c.state, p.state) AS state,
                            p.phone_number,
                            p.birth_date
                        FROM `de-07-kirill-mizko.silver.customers` c
                        LEFT JOIN `de-07-kirill-mizko.silver.user_profiles` p
                        ON c.email = p.email
                    ) S
                    ON T.client_id = S.client_id
                    WHEN MATCHED THEN
                      UPDATE SET
                        first_name = S.first_name,
                        last_name = S.last_name,
                        state = S.state,
                        phone_number = S.phone_number,
                        birth_date = S.birth_date
                    WHEN NOT MATCHED THEN
                      INSERT (
                        client_id, first_name, last_name, email, registration_date, state, phone_number, birth_date
                      )
                      VALUES (
                        S.client_id, S.first_name, S.last_name, S.email, S.registration_date, S.state, S.phone_number, S.birth_date
                      );
                    
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )