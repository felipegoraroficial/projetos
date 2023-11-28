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



account_name = 'nome do seu datalake'
account_key = 'sua chave'
containername_produce = 'nome do container'
transport = RequestsTransport(connection_verify=False)

def load_data_backup():

    def ler_blob_1_sheet(account_name, account_key, containername_produce):
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                                credential=account_key,
                                                transport=transport)
        container_client = blob_service_client.get_container_client(containername_produce)

        dfs = []  # Lista para armazenar os dataframes de cada arquivo

        # Listando blobs no container
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            blob_name = blob.name

            # Download do blob e leitura do DataFrame
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
            df = pd.read_excel(io.BytesIO(blob_data), header=0, engine="openpyxl")

            # Adicionando o DataFrame à lista
            dfs.append(df)

        return dfs
    data_list = ler_blob_1_sheet(account_name, account_key, containername_produce)

    def carregar_dados(df, table_name):
        # Configuração de conexão com o banco de dados
        host = 'seu host'
        banco_de_dados = 'nome do seu banco de dados'
        usuario = 'seu usuário'
        senha = 'sua senha'

        # Criação da string de conexão
        conexao_str = f"mysql+mysqlconnector://{usuario}:{senha}@{host}/{banco_de_dados}"

        # Criar uma engine do SQLAlchemy
        engine = create_engine(conexao_str)

        # Ajustar colunas do DataFrame se necessário
        df.columns = [col.replace(" ", "_") for col in df.columns]

        # Deletar dados existentes na tabela antes de inserir novos dados
        try:
            with engine.connect() as conn:
                delete_query = f"DELETE FROM {table_name};"
                conn.execute(delete_query)
                print(f"Previous data in table '{table_name}' deleted successfully")
        except Exception as e:
            print(f"Error during deletion from table '{table_name}': {e}")

        # Inserir dados na tabela usando o SQLAlchemy
        try:
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"Data for table '{table_name}' inserted successfully")
        except Exception as e:
            print(f"Error during insertion into table '{table_name}': {e}")

        # Fechar a conexão
        engine.dispose()
    # Carregar dados para diferentes tabelas
    carregar_dados(data_list[3], 'FIFA_PAIS')
    carregar_dados(data_list[2], 'FIFA_LIGA')
    carregar_dados(data_list[0], 'FIFA_CLUBE')
    carregar_dados(data_list[1], 'FIFA_JOGADOR')




