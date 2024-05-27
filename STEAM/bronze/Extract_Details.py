# Databricks notebook source
import requests

# Substitua 'YOUR_STEAM_API_KEY' pela sua chave de API da Steam
api_key = '1A1F4922939AFD7DFB99B30C31BCE24F'

# URL base para chamadas da API da Steam
base_url = 'http://api.steampowered.com/'

# Função para obter a lista de todos os aplicativos e jogos da Steam
def get_all_steam_apps(api_key):
    """
    Obtém a lista de todos os aplicativos e jogos disponíveis na Steam.

    Args:
        api_key (str): Chave de API da Steam.

    Returns:
        dict: Um dicionário contendo a lista de todos os aplicativos e jogos.
    """
    endpoint = f'{base_url}ISteamApps/GetAppList/v0002/'
    params = {
        'key': api_key,
        'format': 'json'
    }
    response = requests.get(endpoint, params=params)
    return response.json()

# Função para obter detalhes de um jogo específico
def get_app_details(app_id):
    """
    Obtém os detalhes de um aplicativo ou jogo específico da Steam.

    Args:
        app_id (int): ID do aplicativo ou jogo na Steam.

    Returns:
        dict: Um dicionário contendo os detalhes do aplicativo ou jogo, ou None se a resposta não for um JSON válido ou estiver vazia.
    """
    store_url = 'http://store.steampowered.com/api/appdetails'
    params = {
        'appids': app_id
    }
    response = requests.get(store_url, params=params)
    if response.text.strip():  # Verifica se a resposta não está vazia
        try:
            return response.json()
        except ValueError:  # Captura o erro se a resposta não for um JSON válido
            print(f"A resposta para o appid {app_id} não é um JSON válido.")
            return None
    else:
        print(f"A resposta para o appid {app_id} está vazia.")
        return None

# Exemplo de uso
all_apps = get_all_steam_apps(api_key)

# Filtrar apenas os aplicativos com nomes não vazios
filtered_apps = [app for app in all_apps['applist']['apps'] if app['name'].strip()]

# Transformar appid para o tipo int
filtered_apps_int = [app['appid'] for app in filtered_apps if isinstance(app['appid'], int)]


# COMMAND ----------

# Importação das bibliotecas necessárias
from steam_web_api import Steam
import json

# Chave da API Steam (substitua pela sua própria chave)
KEY = "1A1F4922939AFD7DFB99B30C31BCE24F"
steam = Steam(KEY)

# Lista de IDs de aplicativos a serem buscados
filtered_apps_int = []  # Substitua com os IDs de aplicativos desejados

# Lista para armazenar os detalhes dos aplicativos
app_details_list = []

# Loop para buscar os detalhes de cada aplicativo na lista
for app_id in filtered_apps_int:
    try:
        # Obtenção dos detalhes do aplicativo usando a API Steam
        details = steam.apps.get_app_details(app_id)
        # Verificação se a obtenção foi bem-sucedida
        if 'success' in details and not details['success']:
            print(f"Detalhes do aplicativo {app_id} não encontrados.")
        else:
            # Adição dos detalhes obtidos à lista
            app_details_list.append(details)
            print(f"Detalhes do aplicativo {app_id} adicionados.")
    except Exception as e:
        # Tratamento de erros durante a obtenção dos detalhes
        print(f"Erro ao buscar detalhes para o aplicativo {app_id}: {e}")

# Caminho do arquivo onde os detalhes serão salvos
file_path = r'/lakehouse/default/Files/Raw/steam_details.json'

# Salvamento da lista de detalhes de aplicativos em um arquivo JSON
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(app_details_list, f, ensure_ascii=False, indent=4)

# Mensagem de confirmação do salvamento
print(f"Lista salva em {file_path}")

