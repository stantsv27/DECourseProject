from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime


with DAG(
    dag_id="final_process_user_profiles",
    start_date=datetime(2022, 8, 1),
    schedule_interval=None,
    max_active_runs=1
) as dag:

    start = DummyOperator(task_id='start')

    transfer_to_bq = GCSToBigQueryOperator(
        task_id='transfer_to_bq',
        bucket='raw_data_final_project',
        source_objects="user_profiles/user_profiles.json",
        destination_project_dataset_table='de-07-stas-tsvietkov.bronze.user_profile_raw',
        schema_fields=[
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'birth_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='gc-de'
    )

    transfer_to_silver = BigQueryInsertJobOperator(
        task_id='transfer_to_silver',
        dag=dag,
        configuration={
            "query": {
                "query": "{% include 'sql/user_profiles_to_silver.sql' %}",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": 'de-07-stas-tsvietkov',
                    "datasetId": 'silver',
                    "tableId": 'user_profiles'
                }
            }
        },
        gcp_conn_id='gc-de'
    )

    end = DummyOperator(task_id='end')

    start >> transfer_to_bq >> transfer_to_silver >> end
