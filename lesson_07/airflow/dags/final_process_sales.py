from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime


with DAG(
    dag_id="final_process_sales",
    start_date=datetime(2022, 9, 1),
    end_date=datetime(2022, 10, 1),
    schedule_interval='0 2 * * *',
    max_active_runs=1,
    catchup=True
) as dag:

    start = DummyOperator(task_id='start')

    transfer_to_bq = GCSToBigQueryOperator(
        task_id='transfer_to_bq',
        bucket='raw_data_final_project',
        source_objects=f"sales/{{{{macros.datetime.strptime( ds , '%Y-%m-%d').strftime('%Y-%m-%-d')}}}}"
                       f"/{{{{macros.datetime.strptime( ds , '%Y-%m-%d').strftime('%Y-%m-%-d')}}}}__sales.csv",
        destination_project_dataset_table='de-07-stas-tsvietkov.bronze.sales_raw',
        schema_fields=[
            {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        gcp_conn_id='gc-de'
    )

    transfer_to_silver = BigQueryInsertJobOperator(
        task_id='transfer_to_silver',
        dag=dag,
        configuration={
            "query": {
                "query": "{% include 'sql/transfer_sales_to_silver.sql' %}",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                  "projectId": 'de-07-stas-tsvietkov',
                  "datasetId": 'silver',
                  "tableId": 'sales'
                }
            }
        },
        gcp_conn_id='gc-de'
    )

    end = DummyOperator(task_id='end')

    start >> transfer_to_bq >> transfer_to_silver >> end
