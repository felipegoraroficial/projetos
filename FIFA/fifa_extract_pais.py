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



api_key = "sua key" 
headers = {"X-AUTH-TOKEN": api_key}

account_name = 'seu datalake'
account_key = 'sua key'
containername_raw = 'seu container'
containername_produce = 'seu container'
containername_imagens = 'seu container'
transport = RequestsTransport(connection_verify = False)

def extract_api_pais():

    def nacao():

        nacao_list = list()

        for i in range(1,12):

            url = f"https://futdb.app/api/nations?page={i}" 
                
                
            response = json.loads(requests.request("GET", url=url, headers=headers).text)


            for item in response['items']:

                infos = {
                        "ID Nacionalidade": item['id'],
                        "Nacionalidade": item['name'],
                        }
                    
                nacao_list.append(infos)

        df = pd.DataFrame(nacao_list)

        return df
    nacao_data = nacao()


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
    salvar_arquivo_raw(nacao_data, 'nacao_data.xlsx')


    def imagem_pais(nacao_data):
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
            transport=transport
        )
        container_client = blob_service_client.get_container_client(containername_imagens)

        pais_imagens = []

        for pais_info in nacao_data.to_dict('records'):
            pais_id = pais_info['ID Nacionalidade']
            pais_name = pais_info['Nacionalidade']
            url_imagem = f"https://futdb.app/api/nations/{pais_id}/image"
            response = requests.get(url_imagem, headers=headers)

            if response.status_code == 200:
                blob_client = container_client.get_blob_client(f"pais/{pais_id}.jpg")

                blob_client.upload_blob(response.content, blob_type="BlockBlob", overwrite=True)

                pais_imagens.append({'ID do Clube': pais_id, 'Nome do Clube': pais_name, 'URL do Blob': blob_client.url})
                print(f"Imagem de {pais_name} salva no Azure Blob Storage no diretório 'clube'")
            else:
                print(f"Erro ao acessar a API para {pais_name}: {response.status_code}")

        return pais_imagens
    imagens_salvas = imagem_pais(nacao_data)
