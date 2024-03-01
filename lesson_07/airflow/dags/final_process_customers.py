from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime


with DAG(
    dag_id="final_process_customers",
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 6),
    schedule_interval='0 2 * * *',
    max_active_runs=1,
    catchup=True
) as dag:

    start = DummyOperator(task_id='start')

    transfer_to_bq = GCSToBigQueryOperator(
        task_id='transfer_to_bq',
        bucket='raw_data_final_project',
        source_objects=f"customers/{{{{macros.datetime.strptime( ds , '%Y-%m-%d').strftime('%Y-%m-%-d')}}}}/*.csv",
        destination_project_dataset_table='de-07-stas-tsvietkov.bronze.customers_raw',
        schema_fields=[
            {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='gc-de'
    )

    transfer_to_silver = BigQueryExecuteQueryOperator(
        task_id='transfer_to_silver',
        dag=dag,
        sql="{% include 'sql/merge_users_to_silver.sql' %}",
        gcp_conn_id='gc-de',
        use_legacy_sql=False
    )

    end = DummyOperator(task_id='end')

    start >> transfer_to_bq >> transfer_to_silver >> end
