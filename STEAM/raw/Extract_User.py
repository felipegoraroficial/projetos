# Databricks notebook source
import requests  # Importa o módulo requests para fazer requisições HTTP
import json      # Importa o módulo json para lidar com dados JSON
import datetime  # Importa o módulo datetime para incluir a data atual no arquivo
import pandas as pd

# COMMAND ----------

# Substitua 'YOUR_API_KEY' pela sua chave da API da Steam
API_KEY = '1A1F4922939AFD7DFB99B30C31BCE24F'
STEAM_ID = '76561198991169245'  # Substitua pelo ID Steam do seu perfil

# Endpoint para obter os jogos possuídos por um usuário
url = f'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={API_KEY}&steamid={STEAM_ID}&format=json'

# COMMAND ----------

# Fazendo a requisição GET para a URL da API
response = requests.get(url)

# Converte a resposta da requisição para o formato JSON
data = response.json()

# Acessa a lista de jogos na resposta da API
games = data['response']['games']

# COMMAND ----------

game_user_list = []

for data in games:
    appid = data['appid']
    app_details_response = requests.get(f'https://store.steampowered.com/api/appdetails?appids={appid}')
    app_details_data = app_details_response.json() # Converte a resposta da requisição para JSON e armazena na variável app_details_data
    game_user_list.append(app_details_data) # Adiciona os detalhes do aplicativo à lista game_user_list

# COMMAND ----------

game_data_user = []

# Iterar sobre cada resposta
for response in game_user_list:
    for app_id, data in response.items():
        if 'data' in data:
            game_data = {
                'steam_appid': data['data'].get('steam_appid'),
                'type': data['data'].get('type'),
                'name': data['data'].get('name'),
                'required_age': data['data'].get('required_age'),
                'is_free': data['data'].get('is_free'),
                'header_image': data['data'].get('header_image'),
                'website': data['data'].get('website'),
                'pc_requirements': data['data'].get('pc_requirements'),
                'package_groups': data['data'].get('package_groups'),
                'short_description': data['data'].get('short_description')
            }
            game_data_user.append(game_data)

# COMMAND ----------

# Convert the lists to Pandas DataFrames
games_df = pd.DataFrame(games)
final_data_user_df = pd.DataFrame(game_data_user)

# Merge the DataFrames based on the ID fields
merged_data = pd.merge(games_df, final_data_user_df, left_on='appid', right_on='steam_appid', how='inner')
merged_data.drop('steam_appid', axis=1, inplace=True)
# Convert the merged DataFrame back to a list of dictionaries
final_user_data = merged_data.to_dict('records')

# COMMAND ----------

# Caminho do arquivo para salvar as informações de usuário em formato JSON
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
file_path = f'/dbfs/raw/steam/user/user_steam_{current_date}.json'

# Salva a lista de informações de usuário em um arquivo JSON
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(final_user_data, f, ensure_ascii=False, indent=4)

# Imprime a localização onde o arquivo foi salvo
print(f"Lista salva em {file_path}")
