import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from azure.core.pipeline.transport import RequestsTransport
from io import BytesIO
import io

account_name = 'fececadatalake'
account_key = 'VAz99hLfUkfUjpBRV2hw4TdmSZj2VDxc3pPcETbKBwiOa2/OLxHJlU+mO5NrFZMixXKZCRUV16+A+ASt2GsA8A=='
containername_produce = 'produce'
transport = RequestsTransport(connection_verify=False)

def load_data():

    def ler_blob_1_sheet(account_name, account_key, containername_produce):
        blob_name = 'fifa.xlsx'

        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                                credential=account_key,
                                                transport=transport)
        container_client = blob_service_client.get_container_client(containername_produce)
        blob_client = container_client.get_blob_client(blob_name)

        blob_data = blob_client.download_blob().readall()
        df = pd.read_excel(io.BytesIO(blob_data), header=0, engine="openpyxl")

        return df
    fifa = ler_blob_1_sheet(account_name, account_key, containername_produce)

    # Configuração de conexão com o banco de dados
    host = 'localhost'
    banco_de_dados = 'projetos'
    usuario = 'root'
    senha = 'Fececa13'

    # Criação da string de conexão
    conexao_str = f"mysql+mysqlconnector://{usuario}:{senha}@{host}/{banco_de_dados}"

    # Criar uma engine do SQLAlchemy
    engine = create_engine(conexao_str)


    # Ajustar colunas do DataFrame se necessário
    fifa.columns = [col.replace(" ", "_") for col in fifa.columns]

    # Inserir dados na tabela usando o SQLAlchemy
    try:
        # Substitua 'fifa' pelo nome do seu DataFrame
        fifa.to_sql('FIFA', con=engine, if_exists='replace', index=False)
        print("Data inserted successfully")
    except Exception as e:
        print(f"Error during insertion: {e}")

    # Fechar a conexão
    engine.dispose()
    print("Done.")




