# Databricks notebook source
import requests  # Importa o módulo requests para fazer requisições HTTP
from requests.exceptions import Timeout
import json      # Importa o módulo json para lidar com dados JSON
import datetime  # Importa o módulo datetime para incluir a data atual no arquivo

# COMMAND ----------

# Passo 1: Obter a lista de jogos
response = requests.get('https://api.steampowered.com/ISteamApps/GetAppList/v2/')  # Faz uma requisição GET para obter a lista de aplicativos da Steam
app_list = response.json()  # Converte a resposta da requisição para JSON e armazena na variável app_list

# Lista para armazenar os detalhes dos jogos
game_details_list = []

# COMMAND ----------

# Itera sobre os aplicativos da lista obtida
for app in app_list['applist']['apps']:
    if app['name'] != '':  # Verifica se o nome do aplicativo não está vazio
        appid = app['appid']  # Obtém o ID do aplicativo
        try:
            app_details_response = requests.get(f'https://store.steampowered.com/api/appdetails?appids={appid}', timeout=5)  # Faz uma requisição GET para obter os detalhes do aplicativo usando seu ID com um timeout de 5 segundos
            # Verifica se a requisição para obter os detalhes do aplicativo foi bem-sucedida
            if app_details_response.status_code == 200:
                try:
                    app_details_data = app_details_response.json() # Converte a resposta da requisição para JSON e armazena na variável app_details_data
                    game_details_list.append(app_details_data) # Adiciona os detalhes do aplicativo à lista game_details_list
                except:
                    pass
        except Timeout:
            pass

# COMMAND ----------

# Caminho do arquivo para salvar os detalhes dos jogos em formato JSON
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
file_path = f'/dbfs/raw/steam/games/games_steam_{current_date}.json'

with open(file_path, 'w', encoding='utf-8') as json_file:
    json.dump(game_details_list, json_file, ensure_ascii=False, indent=4)

# Imprime a localização onde o arquivo foi salvo
print(f"Lista salva em {file_path}")
