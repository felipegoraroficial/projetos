from steam_web_api import Steam
import pandas as pd
import json

KEY = "1A1F4922939AFD7DFB99B30C31BCE24F"
steam = Steam(KEY)

# Começar com um ID alto e trabalhar para baixo
current_app_id = 100  # Comece com um ID alto
last_valid_app_id = 0
batch_size = 1  # Quantidade de IDs para verificar de uma vez

# Lista para armazenar os detalhes dos aplicativos válidos
app_details_list = []

while True:
    # Verificar um lote de IDs de aplicativos
    app_details_batch = steam.apps.get_app_details(current_app_id - batch_size, current_app_id)
    
    # Verificar se os detalhes do aplicativo foram retornados
    if app_details_batch:
        # Verificar cada ID de aplicativo no lote
        for app_id, details in app_details_batch.items():
            if 'success' in details and not details['success']:
                # ID não existe, continue para o próximo ID
                continue
            if details:
                last_valid_app_id = app_id
                app_details_list.append(details)  # Adicionar detalhes à lista
                print(steam.apps.get_app_details(last_valid_app_id))
    else:
        # Nenhum detalhe de aplicativo retornado, então provavelmente atingimos o último ID válido
        break
    
    # Atualizar o ID atual para o próximo lote
    current_app_id -= batch_size

print(f"Último ID válido: {last_valid_app_id}")

# Converter a lista de detalhes dos aplicativos em um DataFrame do pandas
df = pd.DataFrame(app_details_list)

# Expandir o dicionário aninhado dentro da coluna 'data' em colunas separadas
df_expanded = pd.json_normalize(df['data'])

# Combinar o DataFrame original com o DataFrame expandido
df_final = pd.concat([df[['success']], df_expanded], axis=1)

# Exibir o DataFrame final
print(df_final)

# Caminho para salvar o arquivo JSON
file_path = r'C:\\Users\\felip\\OneDrive\\Cursos e Certificados\\Data Scientis\\app_details.json'

# Salvar a lista em formato JSON no caminho especificado
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(app_details_list, f, ensure_ascii=False, indent=4)

print(f"Lista salva em {file_path}")


