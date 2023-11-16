import requests
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient,BlobClient,ContainerClient,__version__
from azure.core.pipeline.transport import RequestsTransport
import io
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



api_key = "7b1b94b8-78ca-4f43-b31c-9b3f4199a1a4" 

account_name = 'nome do seu datalake'
account_key = 'sua chave'
containername_raw = 'nome do seu container'
containername_produce = 'nome do seu container'
transport = RequestsTransport(connection_verify = False)

def extract_api():

    def nacao():

        nacao_list = list()

        for i in range(1,12):

            url = f"https://futdb.app/api/nations?page={i}" 
                


            headers = {"X-AUTH-TOKEN": api_key}
                
            response = json.loads(requests.request("GET", url=url, headers=headers).text)


            for item in response['items']:

                infos = {
                        "ID Nacionalidade": item['id'],
                        "Nacionalidade": item['name'],
                        }
                    
                nacao_list.append(infos)

        for item in nacao_list:
            national_id = item['ID Nacionalidade']
            url_imagem = f"https://futdb.app/api/nations/{national_id}/image"
            response = requests.get(url_imagem, headers=headers)

            image_data = response.content
            item['Imagem Pais'] = image_data


        df = pd.DataFrame(nacao_list)

        return df
    nacao_data = nacao()

    def liga():

        liga_list = list()

        for i in range(1,5):

            url = f"https://futdb.app/api/leagues?page={i}" 
            


            headers = {"X-AUTH-TOKEN": api_key}
            
            response = json.loads(requests.request("GET", url=url, headers=headers).text)


            for item in response['items']:

                infos = {
                    "ID Liga": item['id'],
                    "Liga": item['name'],
                    "ID Nacionalidade": item['nationId'],
                    "Genero": item['gender'],
                    }
                
                liga_list.append(infos)

        for item in liga_list:
            league_id = item['ID Liga']
            url_imagem = f"https://futdb.app/api/leagues/{league_id}/image"
            response = requests.get(url_imagem, headers=headers)

            image_data = response.content
            item['Imagem Liga'] = image_data

        df = pd.DataFrame(liga_list)

        return df
    liga_data = liga()

    def clube():

        clube_list = list()

        for i in range(1,39):

            url = f"https://futdb.app/api/clubs?page={i}" 
            


            headers = {"X-AUTH-TOKEN": api_key}
            
            response = json.loads(requests.request("GET", url=url, headers=headers).text)


            for item in response['items']:

                infos = {
                    "ID Clube": item['id'],
                    "Clube": item['name'],
                    "ID Liga": item['league'],
                    }
                
                clube_list.append(infos)

        for item in clube_list:
            club_id = item['ID Clube']
            url_imagem = f"https://futdb.app/api/clubs/{club_id}/image"
            response = requests.get(url_imagem, headers=headers)

            image_data = response.content
            item['Imagem Clube'] = image_data

        df = pd.DataFrame(clube_list)

        return df
    clube_data = clube()

    def jogadores():

        jogadores_list = list()

        for i in range(1,929):

            url = f"https://futdb.app/api/players?page={i}" 


            headers = {"X-AUTH-TOKEN": api_key}
            
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
                
        for item in jogadores_list:
            player_id = item['ID']
            url_imagem = f"https://futdb.app/api/players/{player_id}/image"
            response = requests.get(url_imagem, headers=headers)

            image_data = response.content
            item['Imagem Jogador'] = image_data

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
    salvar_arquivo_raw(nacao_data, 'nacao_data.xlsx')
    salvar_arquivo_raw(liga_data, 'liga_data.xlsx')
    salvar_arquivo_raw(clube_data, 'clube_data.xlsx')
    salvar_arquivo_raw(jogadores_data, 'jogadores_data.xlsx')
