# Databricks notebook source
import glob
import json
import os

# COMMAND ----------

# obter todos os arquivos JSON no DBFS
file_list = glob.glob("/dbfs/raw/steam/user/*.json")

# Criando lista vazia para armazenar todos arquivos JSON
all_data = []

# Ler cada arquivo json e agrupar na lista vazia
for file in file_list: #iterar sobre a lista
    with open(file, 'r') as f: #abrir arquivo json em modo de leitura
        json_list = json.load(f) #carregar o conteúdo do arquivo JSON em uma lista de objetos JSON
        for json_obj in json_list: # iterar sobre cada objeto JSON na lista
            file_name = os.path.basename(file) #obter o nome do arquivo sem o caminho
            date_value = file_name.split('_')[-1].split('.')[0] #extrair a data do nome do arquivo
            json_obj['file_data'] = date_value #adicionar a chave 'file_data' ao objeto JSON com o valor da data
        all_data.extend(json_list) #adicionar os objetos JSON à lista all_data

# COMMAND ----------

# Caminho do arquivo para salvar as informações de usuário em formato JSON
file_path = r'/dbfs/bronze/steam/user/user_steam.json'

# Salva a lista de informações de usuário em um arquivo JSON
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(all_data, f, ensure_ascii=False, indent=4)

# Imprime a localização onde o arquivo foi salvo
print(f"Lista salva em {file_path}")
