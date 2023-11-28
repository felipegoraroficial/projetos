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


def extract_api_clube():


    def clube():

        clube_list = list()

        for i in range(1,39):

            url = f"https://futdb.app/api/clubs?page={i}" 
            
            
            response = json.loads(requests.request("GET", url=url, headers=headers).text)


            for item in response['items']:

                infos = {
                    "ID Clube": item['id'],
                    "Clube": item['name'],
                    "ID Liga": item['league'],
                    }
                
                clube_list.append(infos)

        df = pd.DataFrame(clube_list)

        return df
    clube_data = clube()


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
    salvar_arquivo_raw(clube_data, 'clube_data.xlsx')


    def imagem_clube(clube_data):
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
            transport=transport
        )
        container_client = blob_service_client.get_container_client(containername_imagens)

        clube_imagens = []

        for club_info in clube_data.to_dict('records'):
            club_id = club_info['ID Clube']
            club_name = club_info['Clube']
            url_imagem = f"https://futdb.app/api/clubs/{club_id}/image"
            response = requests.get(url_imagem, headers=headers)

            if response.status_code == 200:
                blob_client = container_client.get_blob_client(f"clube/{club_id}.jpg")

                blob_client.upload_blob(response.content, blob_type="BlockBlob", overwrite=True)

                clube_imagens.append({'ID do Clube': club_id, 'Nome do Clube': club_name, 'URL do Blob': blob_client.url})
                print(f"Imagem de {club_name} salva no Azure Blob Storage no diretório 'clube'")
            else:
                print(f"Erro ao acessar a API para {club_name}: {response.status_code}")

        return clube_imagens
    imagens_salvas = imagem_clube(clube_data)