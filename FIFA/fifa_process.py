from airflow import DAG
from datetime import datetime
import pendulum
from fifa_extract_pais import extract_api_pais
from fifa_extract_liga import extract_api_liga
from fifa_extract_clube import extract_api_clube
from fifa_extract_jogador import extract_api_players
from fifa_transform import tratar_dados
from fifa_load_backup import load_data_backup
from fifa_load_produce import load_data_produce
from airflow.operators.python import PythonOperator


# Defina o fuso horário desejado (São Paulo, Brasil)
local_tz = pendulum.timezone('America/Sao_Paulo')

default_args = {
    "owner": "seu usuário",
    "start_date": datetime(2023, 11, 9, tzinfo=local_tz),  # Defina o fuso horário para a data de início
}

dag = DAG(
    "fifa_process",
    default_args=default_args,
    schedule_interval="0 6 * * 1-5",
    max_active_runs=1,
    catchup=False  # Defina como False para ignorar a execução das tarefas pendentes
)

extratct_data_pais = PythonOperator(
    task_id='extratct_data_pais',
    python_callable= extract_api_pais,
    dag=dag
)

extratct_data_liga = PythonOperator(
    task_id='extratct_data_liga',
    python_callable= extract_api_liga,
    dag=dag
)

extratct_data_clube = PythonOperator(
    task_id='extratct_data_clube',
    python_callable= extract_api_clube,
    dag=dag
)

extratct_data_jogador = PythonOperator(
    task_id='extratct_data_jogador',
    python_callable= extract_api_players,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable= tratar_dados,
    dag=dag
)

carregar_data_backup = PythonOperator(
    task_id='carregar_data_backup',
    python_callable= load_data_backup,
    dag=dag
)

carregar_data_produce = PythonOperator(
    task_id='carregar_data_produce',
    python_callable= load_data_produce,
    dag=dag
)


[extratct_data_pais, extratct_data_liga, extratct_data_clube, extratct_data_jogador] >> transform_data >> [carregar_data_backup, carregar_data_produce]