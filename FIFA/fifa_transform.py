import pandas as pd
from azure.storage.blob import BlobServiceClient,BlobClient,ContainerClient,__version__
from azure.core.pipeline.transport import RequestsTransport
from io import BytesIO
import io
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


account_name = 'fececadatalake'
account_key = 'VAz99hLfUkfUjpBRV2hw4TdmSZj2VDxc3pPcETbKBwiOa2/OLxHJlU+mO5NrFZMixXKZCRUV16+A+ASt2GsA8A=='
containername_raw = 'raw'
containername_produce = 'produce'
transport = RequestsTransport(connection_verify = False)


def tratar_dados():

    def ler_blob_1_sheet(account_name, account_key, containername_raw):
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                                credential=account_key,
                                                transport=transport)
        container_client = blob_service_client.get_container_client(containername_raw)

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
    data_list = ler_blob_1_sheet(account_name, account_key, containername_raw)

    def tratar_dados(data_list):

        nacao_data = data_list[3]
        liga_data = data_list[2]
        clube_data = data_list[0]
        jogadores_data = data_list[1]

        # Mesclagem encadeada dos dataframes
        fifa = jogadores_data.merge(clube_data[['ID Clube', 'Clube']], left_on='ID Clube', right_on='ID Clube', how='left')\
                    .merge(liga_data[['ID Liga', 'Liga', 'Genero']], left_on='ID Liga', right_on='ID Liga', how='left')\
                    .rename(columns={'Genero_x': 'Genero', 'Genero_y': 'Genero Liga'})\
                    .merge(nacao_data[['ID Nacionalidade', 'Nacionalidade']], left_on='ID Nacionalidade', right_on='ID Nacionalidade', how='left')

        # Preenchimento de valores NaN
        fifa.fillna({'Genero Liga': 'Não se Aplica', 'Atributo de Goleiro': 0, 'IMC': 0, 'Peso':0, 'Genero':'-'}, inplace=True)
        fifa = fifa.drop(['ID Clube', 'ID Liga', 'ID Nacionalidade'], axis=1)

        # Conversão de tipos
        fifa['Atributo de Goleiro'] = fifa['Atributo de Goleiro'].astype(int)
        fifa['Altura'] = fifa['Altura'] / 100

        # Cálculo do IMC
        def calcular_imc(row):
            if row['Peso'] == 0 and row['Altura'] == 0:
                return 0
            elif row['Peso'] != 0 and row['Altura'] == 0:
                return 0
            else:
                return round(row['Peso'] / (row['Altura'] ** 2), 2)

        fifa['IMC'] = fifa.apply(calcular_imc, axis=1)

        # Classificação do IMC
        def classificar_imc(imc):
            if imc == 0:
                return "Não se Aplica"
            elif 0 < imc < 18.5:
                return "Abaixo do Peso"
            elif 18.5 <= imc < 24.9:
                return "Peso Normal"
            elif 24.9 <= imc < 29.9:
                return "Sobrepeso"
            else:
                return "Obeso"

        fifa['Classificacao IMC'] = fifa['IMC'].apply(classificar_imc)

        return fifa
    fifa = tratar_dados(data_list)

    def salvar_arquivo_produce(df, blob_name):
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
            transport=transport
        )
        container_client = blob_service_client.get_container_client(containername_produce)
        blob_client = container_client.get_blob_client(blob_name)

        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        blob_client.upload_blob(output, overwrite=True)
    salvar_arquivo_produce(fifa, 'fifa.xlsx')
