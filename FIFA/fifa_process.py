from airflow import DAG
from datetime import datetime
from fifa_extract import extract_api
from fifa_transform import tratar_dados
from fifa_load import load_data
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "teste",
    "start_date": datetime(2023, 11, 9),
}

dag = DAG(
    "fifa_process",
    default_args=default_args,
    schedule_interval="0 6 * * 1-5",
    max_active_runs=1,
)

extratct_data = PythonOperator(
    task_id='extratct_data',
    python_callable= extract_api,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable= tratar_dados,
    dag=dag
)

carregar_data = PythonOperator(
    task_id='carregar_data',
    python_callable= load_data,
    dag=dag
)


extratct_data >> transform_data >> carregar_data