from google.cloud import storage
import pandas as pd
import numpy as np
import re
from io import StringIO
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

google_credencial = storage.Client.from_service_account_json('')
bucket_name = ""
folder_name = ""
file_name = ""
folder_name_gold = ""
file_name_gold = ""

def gold_data_magalu():

    def read_file_bucket(bucket_name, folder_name, file_name, google_credencial):
        storage_client = google_credencial

        bucket = storage_client.get_bucket(bucket_name)

        blob = bucket.blob(folder_name + file_name)

        content = blob.download_as_text()
        content_io = StringIO(content)

        df_ini = pd.read_json(content_io)

        return df_ini

    df_ini = read_file_bucket(bucket_name, folder_name, file_name, google_credencial)

    df_final = df_ini.copy()

    df_final['preco_promo'] = df_final['preco_promo'].astype(np.float64).round(2)

    def extrair_memoria(info):
            # Usar regex para encontrar padrões "GB" ou "Gb" ou "gb" seguidos por números
        padrao = r'(\d+)\s*(G[gBb])'
        resultado = re.search(padrao, info)
        if resultado:
            # Se encontrado, retornar a correspondência
            return resultado.group(0)
        else:
            # Se não encontrado, retornar uma string vazia
            return '-'

    # Aplicar a função e criar a nova coluna "Memoria"
    df_final['memoria'] = df_final['titulo'].apply(extrair_memoria)

    oled = df_final['titulo'].str.contains('Oled', case=False)
    oled = oled.replace({True: 'Sim', False: 'Nao'})
    df_final['oled'] = oled

    lite = df_final['titulo'].str.contains('Lite', case=False)
    lite = lite.replace({True: 'Sim', False: 'Nao'})
    df_final['lite'] = lite

    controle = df_final['titulo'].str.contains('Joy-con', case=False)
    controle = controle.replace({True: 'Sim', False: 'Nao'})
    df_final['joy_con'] = controle

    df_final['Empresa'] = 'Magalu' 

    return df_ini, df_final

df_ini, df_final = gold_data_magalu()

# Salvar o DataFrame em um arquivo Parquet
parquet_bytes_io = BytesIO()
table = pa.Table.from_pandas(df_final)
pq.write_table(table, parquet_bytes_io, compression='SNAPPY', flavor='spark', use_dictionary=True, coerce_timestamps='ms')
parquet_bytes_io.seek(0)  # Voltar para o início do BytesIO

# Credenciais de autenticação do Google Cloud Storage
storage_client = google_credencial
bucket = storage_client.get_bucket(bucket_name)

# Caminho do arquivo Parquet dentro da pasta
blob = bucket.blob(folder_name_gold + file_name_gold)

# Upload do arquivo Parquet a partir do BytesIO
blob.upload_from_file(parquet_bytes_io, content_type='application/octet-stream')

# Verifique se a lista 'df_final' não está vazia
assert not df_ini.empty, "O DataFrame 'df_ini' está vazio"
# Verifique se a lista 'df_ini' não está vazia
assert not df_final.empty, "O DataFrame 'df_final' está vazio"




