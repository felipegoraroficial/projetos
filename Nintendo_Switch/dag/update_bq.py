from google.cloud import storage
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import os

def update_data():

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

    google_credencial = storage.Client.from_service_account_json('')
    bucket_name = ""
    pasta= ""

    def read_files_and_upload(bucket_name, pasta, arquivos, google_credencial, projeto, conjunto_de_dados):
        storage_client = google_credencial
        for arquivo, tabela in arquivos.items():
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(pasta + arquivo)
            content = blob.download_as_bytes()
            df = pq.read_table(BytesIO(content)).to_pandas()
            upload_to_bigquery(df, projeto, conjunto_de_dados, tabela)

    def upload_to_bigquery(df, projeto, conjunto_de_dados, tabela):
        nome_tabela_completo = f'{projeto}.{conjunto_de_dados}.{tabela}'
        df.to_gbq(destination_table=nome_tabela_completo, project_id=projeto, if_exists='replace')

    arquivos = {
        "kabum-nintendo.parquet": "Kabum",
        "magalu-nintendo.parquet": "Magalu",
        "ml-nintendo.parquet": "Mercado Livre"
    }

    projeto = ''
    conjunto_de_dados = ''

    read_files_and_upload(bucket_name, pasta, arquivos, google_credencial, projeto, conjunto_de_dados)
