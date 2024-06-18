# Databricks notebook source
import glob
import json
import os

# COMMAND ----------

# Obter todos os arquivos JSON no DBFS
file_list = glob.glob("/dbfs/raw/steam/games/*.json")

# Criar lista vazia para armazenar todos os arquivos JSON
all_data = []

# Ler cada arquivo JSON e agrupar na lista vazia
for file in file_list:
    with open(file, 'r') as f:  # Abrir arquivo JSON em modo de leitura
        json_data = json.load(f)  # Carregar o conteúdo do arquivo JSON
        file_name = os.path.basename(file)  # Obter o nome do arquivo sem o caminho
        date_value = file_name.split('_')[-1].split('.')[0]  # Extrair a data do nome do arquivo
        
        # Adicionar a chave 'file_data' ao objeto JSON com o valor no campo de chaves data
        for item in json_data:
            for key, value in item.items():
                value['file_data'] = date_value
        
        all_data.extend(json_data)  # Adicionar os objetos JSON à lista all_data

# COMMAND ----------

# Filtra os resultados verdadeiros
true_list = [dlc for dlc in all_data if list(dlc.values())[0]['success'] != False]

# COMMAND ----------

# Inicializa uma lista vazia para armazenar os dados filtrados
filtered_data = []

# Itera sobre cada item na lista true_list
for item in true_list:
    if isinstance(item, dict):
        # Itera sobre os itens do dicionário interno (key, value)
        for key, value in item.items():
            # Obtém o dicionário 'data' dentro de 'value'; se não existir, usa um dicionário vazio {}
            if isinstance(value, dict):
                data = value.get('data', {})
                
                # Obtém o valor da chave 'type' dentro de 'data'
                item_type = data.get('type')
                
                # Verifica se o valor de 'type' é igual a 'game'
                if item_type == 'game':
                    # Se o tipo for 'game', adiciona o item completo (key: value) à lista filtrada
                    filtered_data.append({key: value})

# COMMAND ----------

final_data = []

# Iterar sobre cada resposta
for response in filtered_data:
    for app_id, data in response.items():
        game_data = {
            'steam_appid': data['data']['steam_appid'],
            'type': data['data']['type'],
            'name': data['data']['name'],
            'required_age': data['data']['required_age'],
            'is_free': data['data']['is_free'],
            'header_image': data['data']['header_image'],
            'website': data['data']['website'],
            'pc_requirements': data['data']['pc_requirements'],
            'package_groups': data['data']['package_groups'],
            'short_description': data['data']['short_description'],
            'file_data': data['file_data']
        }
        final_data.append(game_data)

# COMMAND ----------

# Caminho do arquivo para salvar as informações de usuário em formato JSON
file_path = r'/dbfs/bronze/steam/games/games_steam.json'

# Salva a lista de informações de usuário em um arquivo JSON
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(final_data, f, ensure_ascii=False, indent=4)

# Imprime a localização onde o arquivo foi salvo
print(f"Lista salva em {file_path}")
