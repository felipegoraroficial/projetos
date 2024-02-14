import requests
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient,BlobClient,ContainerClient,__version__
from azure.core.pipeline.transport import RequestsTransport
from io import BytesIO
from PIL import Image
import PIL
import io
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


api_key = "sua chave" 
headers = {"X-AUTH-TOKEN": api_key}

account_name = 'nome do seu datalake'
account_key = 'sua chave'
containername_raw = 'nome do container'
containername_produce = 'nome do container'
containername_imagens = 'nome do container'
transport = RequestsTransport(connection_verify = False)

def extract_api_liga():

    def liga():

        liga_list = list()

        for i in range(1,5):

            url = f"https://futdb.app/api/leagues?page={i}" 
            
            
            response = json.loads(requests.request("GET", url=url, headers=headers).text)


            for item in response['items']:

                infos = {
                    "ID Liga": item['id'],
                    "Liga": item['name'],
                    "ID Nacionalidade": item['nationId'],
                    "Genero": item['gender'],
                    }
                
                liga_list.append(infos)

        df = pd.DataFrame(liga_list)

        return df
    liga_data = liga()


    def salvar_arquivo_raw(df, blob_name):
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
            transport=transport
        )
        container_client = blob_service_client.get_container_client(containername_raw)
        blob_client = container_client.get_blob_client(blob_name)

        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        blob_client.upload_blob(output, overwrite=True)
    salvar_arquivo_raw(liga_data, 'liga_data.xlsx')


    def imagem_liga(liga_data):
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
            transport=transport
        )
        container_client = blob_service_client.get_container_client(containername_imagens)

        liga_imagens = []

        for liga_info in liga_data.to_dict('records'):
            liga_id = liga_info['ID Liga']
            liga_name = liga_info['Liga']
            url_imagem = f"https://futdb.app/api/leagues/{liga_id}/image"
            response = requests.get(url_imagem, headers=headers)

            if response.status_code == 200:
                blob_client = container_client.get_blob_client(f"liga/{liga_id}.jpg")

                blob_client.upload_blob(response.content, blob_type="BlockBlob", overwrite=True)

                liga_imagens.append({'ID do Clube': liga_id, 'Nome do Clube': liga_name, 'URL do Blob': blob_client.url})
                print(f"Imagem de {liga_name} salva no Azure Blob Storage no diretório 'clube'")
            else:
                print(f"Erro ao acessar a API para {liga_name}: {response.status_code}")

        return liga_imagens
    imagens_salvas = imagem_liga(liga_data)