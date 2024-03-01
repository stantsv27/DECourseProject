from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime


with DAG(
    dag_id="final_enrich_user_profiles",
    start_date=datetime(2022, 8, 1),
    schedule_interval=None,
    max_active_runs=1
) as dag:

    start = DummyOperator(task_id='start')

    create_user_profiles_enriched = BigQueryExecuteQueryOperator(
        task_id='create_user_profiles_enriched',
        dag=dag,
        sql="{% include 'sql/create_user_profiles_enriched.sql' %}",
        gcp_conn_id='gc-de',
        use_legacy_sql=False
    )

    enrich_user_profiles = BigQueryExecuteQueryOperator(
        task_id='enrich_user_profiles',
        dag=dag,
        sql="{% include 'sql/enrich_user_profiles.sql' %}",
        gcp_conn_id='gc-de',
        use_legacy_sql=False
    )

    end = DummyOperator(task_id='end')

    start >> create_user_profiles_enriched >> enrich_user_profiles >> end
