from steam_web_api import Steam
import pandas as pd
import json

KEY = "1A1F4922939AFD7DFB99B30C31BCE24F"
steam = Steam(KEY)

current_app_id = 1000000
last_valid_app_id = 0
batch_size = 10

app_details_list = []

while current_app_id >= 0:  # Altere a condição do loop para incluir o ID 0
    try:
        app_details_batch = steam.apps.get_app_details(current_app_id - batch_size, current_app_id)
        
        if app_details_batch:
            for app_id, details in app_details_batch.items():
                if 'success' in details and not details['success']:
                    continue
                if details:
                    last_valid_app_id = app_id
                    app_details_list.append(details)
                    print(f"Detalhes do aplicativo {last_valid_app_id} adicionados.")
        else:
            print(f"Nenhum detalhe retornado para o intervalo {current_app_id - batch_size} - {current_app_id}.")
        
        current_app_id -= batch_size
    except Exception as e:
        print(f"Erro ao buscar detalhes para o intervalo {current_app_id - batch_size} - {current_app_id}: {e}")
        #break

print(f"Último ID válido: {last_valid_app_id}")

df = pd.DataFrame(app_details_list)
df_expanded = pd.json_normalize(df['data'])
df_final = pd.concat([df[['success']], df_expanded], axis=1)

print(df_final)

file_path = r'C:\\Users\\felip\\OneDrive\\Cursos e Certificados\\Data Scientis\\app_details.json'

with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(app_details_list, f, ensure_ascii=False, indent=4)

print(f"Lista salva em {file_path}")


