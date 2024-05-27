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

# Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from steam_web_api import Steam
import json

# Chave da API Steam (substitua pela sua própria chave)
KEY = "1A1F4922939AFD7DFB99B30C31BCE24F"
steam = Steam(KEY)

# Lista para armazenar os jogos encontrados
games = []

# IDs dos jogos a serem buscados (substitua pelos IDs desejados)
filtered_apps = []  # Exemplo: [123456, 234567, 345678]

# Loop para buscar cada jogo pelo ID
for game_id in filtered_apps:
    # Busca o jogo pelo ID usando a API Steam
    user = steam.apps.search_games(game_id)
    # Adiciona o resultado à lista de jogos
    games.append(user)

# Caminho do arquivo onde os jogos serão salvos
file_path = '/lakehouse/default/Files/Raw/steam_games.json'

# Salvamento da lista de jogos em um arquivo JSON
with open(file_path, 'w') as f:
    json.dump(games, f)

# Mensagem de confirmação do salvamento
print(f"Lista de jogos salva em {file_path}")

