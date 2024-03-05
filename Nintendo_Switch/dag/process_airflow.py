from airflow import DAG
from datetime import datetime
import pendulum
from extract_raw_magalu import get_data_magalu
from extract_raw_kabum import get_data_kabum
from extract_raw_ml import get_data_ml
from silver_magalu import silver_data_magalu
from silver_kabum import silver_data_kabum
from silver_ml import silver_data_ml
from gold_magalu import gold_data_magalu
from gold_kabum import gold_data_kabum
from gold_ml import gold_data_ml
from update_bq import update_data
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Defina o fuso horário desejado (São Paulo, Brasil)
local_tz = pendulum.timezone('America/Sao_Paulo')

default_args = {
    "owner": "teste",
    "start_date": datetime(2024, 3, 5, tzinfo=local_tz),  # Defina o fuso horário para a data de início
}

dag = DAG(
    "nintendo_process",
    default_args=default_args,
    schedule_interval="0 6 * * 1-5",
    max_active_runs=1,
    catchup=False  # Defina como False para ignorar a execução das tarefas pendentes
)

# Lista de fontes de dados e transformações
data_sources = [
    ("magalu", get_data_magalu),
    ("kabum", get_data_kabum),
    ("ml", get_data_ml)
]

silver_transformations = [
    ("magalu", silver_data_magalu),
    ("kabum", silver_data_kabum),
    ("ml", silver_data_ml)
]

gold_transformations = [
    ("magalu", gold_data_magalu),
    ("kabum", gold_data_kabum),
    ("ml", gold_data_ml)
]

# Definindo operadores de extração de dados
extract_tasks = []
dummy_before_silver = DummyOperator(task_id="dummy_before_silver", dag=dag)
for source, extraction_func in data_sources:
    task_id = f'extract_data_{source}'
    extract_task = PythonOperator(
        task_id=task_id,
        python_callable=extraction_func,
        dag=dag
    )
    extract_task >> dummy_before_silver
    extract_tasks.append(extract_task)

# Definindo operadores de transformação de dados (silver)
silver_tasks = []
dummy_before_gold = DummyOperator(task_id="dummy_before_gold", dag=dag)
for source, transformation_func in silver_transformations:
    task_id = f'silver_data_{source}'
    silver_task = PythonOperator(
        task_id=task_id,
        python_callable=transformation_func,
        dag=dag
    )
    silver_task >> dummy_before_gold
    silver_tasks.append(silver_task)

# Definindo operadores de transformação de dados (gold)
gold_tasks = []
dummy_before_bigquery = DummyOperator(task_id="dummy_before_bigquery", dag=dag)
for source, transformation_func in gold_transformations:
    task_id = f'gold_data_{source}'
    gold_task = PythonOperator(
        task_id=task_id,
        python_callable=transformation_func,
        dag=dag
    )
    gold_task >> dummy_before_bigquery
    gold_tasks.append(gold_task)

# Define a tarefa para atualização do BigQuery
update_bigquery = PythonOperator(
    task_id='update_bigquery',
    python_callable=update_data,
    dag=dag
)

dummy_before_silver >> silver_tasks
dummy_before_gold >> gold_tasks
dummy_before_bigquery >> update_bigquery


