from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import datetime
from time import sleep
from typing import Dict


def trigger_http_job(http_conn_id: str, parameters: Dict):
    """
    This func triggers http_conn_id with parameters
    :param http_conn_id: airflow connection id
    :param parameters: required parameters for connection id
    :return: None
    """

    hook = HttpHook(method='POST', http_conn_id=http_conn_id)
    res = hook.run(endpoint=None, headers={'Content-Type': 'application/json'}, json=parameters)
    assert res.status_code == 201


def sleep_for_15():
    """
    Sleep  for 15 seconds
    :return: None
    """

    sleep(15)


with DAG(
    dag_id="process_sales",
    start_date=datetime.datetime(2022, 8, 9),
    end_date=datetime.datetime(2022, 8, 12),
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    catchup=True
) as dag:

    start = EmptyOperator(task_id='start')

    task_1 = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=trigger_http_job,
        provide_context=True,
        op_kwargs={
            'http_conn_id': 'localhost_8081',
            'parameters': {
                'date': '{{ ds }}',
                'raw_dir': '/Users/s_tsv/Study/RD Data Engineer/DECourseProject/lesson_02/file_storage/raw/sales/{{ ds }}'
            }
        }
    )

    rest = PythonOperator(
        task_id='rest',
        python_callable=sleep_for_15
    )

    task_2 = PythonOperator(
        task_id='convert_to_avro',
        python_callable=trigger_http_job,
        provide_context=True,
        op_kwargs={
            'http_conn_id': 'localhost_8082',
            'parameters': {
                'stg_dir': '/Users/s_tsv/Study/RD Data Engineer/DECourseProject/lesson_02/file_storage/stg/sales/{{ ds }}',
                'raw_dir': '/Users/s_tsv/Study/RD Data Engineer/DECourseProject/lesson_02/file_storage/raw/sales/{{ ds }}'
            }
        }
    )

    end = EmptyOperator(task_id='end')

    start >> task_1 >> rest >> task_2 >> end
