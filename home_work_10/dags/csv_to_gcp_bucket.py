from airflow.example_dags.example_external_task_marker_dag import start_date
from airflow  import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

gcp_bucket_name = 'rd-de-api'
gcp_dst_path = 'src1/sales/v1/'
local_src_path = '/date/sales/{{execution_date.strftime("%Y-%m-%d")}}/*.csv'


with DAG(
     dag_id = 'csv_to_gcp_bucket'
    ,schedule_interval = '@daily'
    ,start_date = datetime(2022,8,9)
    ,end_date = datetime(2022,8,10)
    ,catchup=True
) as dag:
    task = LocalFilesystemToGCSOperator(
        task_id = 'push_csv_to_gcp',
        bucket = gcp_bucket_name,
        src = local_src_path,
        dst = gcp_dst_path+'{{execution_date.strftime("%Y/%m/%d")}}/',
        mime_type = 'text/plain'
    )