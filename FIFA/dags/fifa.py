from airflow import DAG
from datetime import datetime
import pendulum
from airflow.operators.python import PythonOperator
from tasks.extract.extract_club import obter_total_paginas_clube, clube, imagem_clube
from tasks.extract.extract_league import obter_total_paginas_liga, liga, imagem_liga
from tasks.extract.extract_nations import obter_total_paginas_país, país, imagem_país
from tasks.extract.extract_players import obter_total_paginas_jogador, jogador, imagem_jogador
from tasks.bronze.bronze_club import get_gcs_bucket, list_blobs_with_prefix, read_json_blobs, process_json_data

# Defina o fuso horário desejado (São Paulo, Brasil)
local_tz = pendulum.timezone('America/Sao_Paulo')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 6, 24, tzinfo=local_tz),
}

dag = DAG(
    "fifa",
    default_args=default_args,
    schedule_interval="0 6 * * 1-5",
    max_active_runs=1,
    catchup=False
)

#EXTRACT STEP

pags_club = PythonOperator(
    task_id='total_club_pags',
    python_callable=obter_total_paginas_clube,
    provide_context=True,
    dag=dag
)

pags_league = PythonOperator(
    task_id='total_league_pags',
    python_callable=obter_total_paginas_liga,
    provide_context=True,
    dag=dag
)

pags_nation = PythonOperator(
    task_id='total_nations_pags',
    python_callable=obter_total_paginas_país,
    provide_context=True,
    dag=dag
)

pags_players = PythonOperator(
    task_id='total_players_pags',
    python_callable=obter_total_paginas_jogador,
    provide_context=True,
    dag=dag
)

club_data = PythonOperator(
    task_id='club_data',
    python_callable=clube,
    provide_context=True,
    dag=dag
)

league_data = PythonOperator(
    task_id='league_data',
    python_callable=liga,
    provide_context=True,
    dag=dag
)

nation_data = PythonOperator(
    task_id='nation_data',
    python_callable=país,
    provide_context=True,
    dag=dag
)

players_data = PythonOperator(
    task_id='players_data',
    python_callable=jogador,
    provide_context=True,
    dag=dag
)

img_club = PythonOperator(
    task_id='upload_img_club',
    python_callable=imagem_clube,
    provide_context=True,
    dag=dag
)

img_league = PythonOperator(
    task_id='upload_img_league',
    python_callable=imagem_liga,
    provide_context=True,
    dag=dag
)

img_nation = PythonOperator(
    task_id='upload_img_nation',
    python_callable=imagem_país,
    provide_context=True,
    dag=dag
)

img_players = PythonOperator(
    task_id='upload_img_players',
    python_callable=imagem_jogador,
    provide_context=True,
    dag=dag
)

#BRONZE STEP


task_list_blobs = PythonOperator(
    task_id='list_blobs_with_prefix',
    python_callable=list_blobs_with_prefix,
    provide_context=True, 
    dag=dag
)


task_read_json_blobs = PythonOperator(
    task_id='read_json_blobs',
    python_callable=read_json_blobs,
    provide_context=True,  
    dag=dag
)

task_process_json_data = PythonOperator(
    task_id='process_json_data',
    python_callable=process_json_data,
    provide_context=True,  
    dag=dag
)
    


# Define dependencies within each parallel task list
pags_club >> club_data >> img_club
pags_league >> league_data >> img_league
pags_nation >> nation_data >> img_nation
pags_players >> players_data >> img_players

# Combine all the parallel task lists to run before task_list_blobs
[img_club, img_league, img_nation, img_players] >> task_list_blobs

# Continue with the remaining tasks
task_list_blobs >> task_read_json_blobs >> task_process_json_data
