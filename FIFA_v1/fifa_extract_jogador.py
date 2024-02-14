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

def extract_api_players():

    def jogadores():

        jogadores_list = list()

        for i in range(1,929):

            url = f"https://futdb.app/api/players?page={i}" 
            
            response = json.loads(requests.request("GET", url=url, headers=headers).text)


            for item in response['items']:

                infos = {
                    "ID": item['id'],
                    "ID Clube": item['club'],
                    "ID Liga": item['league'],
                    "ID Nacionalidade": item['nation'],
                    "Nome": item['name'],
                    "Idade": item['age'],
                    "Data Nascimento": item['birthDate'],
                    "Posição": item['position'],
                    "Overall": item['rating'],
                    "Pé Bom": item['foot'],
                    "Overall": item['rating'],
                    "Altura": item['height'],
                    "Peso": item['weight'],
                    "Genero": item['gender'],
                    "Chute": item['shooting'],
                    "Velocidade": item['pace'],
                    "Passe": item['passing'],
                    "Drible": item['dribbling'],
                    "Defesa": item['defending'],
                    "Fisico": item['physicality'],
                    "Atributo de Goleiro": item['goalkeeperAttributes'],
                    }
                
                jogadores_list.append(infos)

        df = pd.DataFrame(jogadores_list)

        return df
    jogadores_data = jogadores()


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
    salvar_arquivo_raw(jogadores_data, 'jogadores_data.xlsx')


    def imagem_jogador(jogadores_data):
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
            transport=transport
        )
        container_client = blob_service_client.get_container_client(containername_imagens)

        liga_imagens = []

        for jogador_info in jogadores_data.to_dict('records'):
            jogador_id = jogador_info['ID']
            jogador_name = jogador_info['Nome']
            url_imagem = f"https://futdb.app/api/players/{jogador_id}/image"
            response = requests.get(url_imagem, headers=headers)

            if response.status_code == 200:
                blob_client = container_client.get_blob_client(f"jogador/{jogador_id}.jpg")

                blob_client.upload_blob(response.content, blob_type="BlockBlob", overwrite=True)

                liga_imagens.append({'ID do Clube': jogador_id, 'Nome do Clube': jogador_name, 'URL do Blob': blob_client.url})
                print(f"Imagem de {jogador_name} salva no Azure Blob Storage no diretório 'clube'")
            else:
                print(f"Erro ao acessar a API para {jogador_name}: {response.status_code}")

        return liga_imagens
    imagens_salvas = imagem_jogador(jogadores_data)
