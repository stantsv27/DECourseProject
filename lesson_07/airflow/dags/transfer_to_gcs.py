from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import datetime


with DAG(
    dag_id="transfer_to_gcs",
    start_date=datetime.datetime(2022, 8, 9),
    end_date=datetime.datetime(2022, 8, 11),
    schedule_interval='0 2 * * *',
    max_active_runs=1,
    catchup=True
) as dag:

    start = DummyOperator(task_id='start')

    transfer_files = LocalFilesystemToGCSOperator(
        task_id='transfer_files',
        src='/Users/s_tsv/Study/RD Data Engineer/DECourseProject/lesson_02/file_storage/stg/sales/{{ ds }}/sales_{{ ds }}.avro',
        dst=f'sales/v1/year={{{{ ds_nodash[0:4] }}}}/month={{{{ ds_nodash[4:6] }}}}/day={{{{ ds_nodash[6:] }}}}/sales_{{{{ ds }}}}.avro',
        bucket='src-api',
        gcp_conn_id='gc-de'
    )

    end = DummyOperator(task_id='end')

    start >> transfer_files >> end
