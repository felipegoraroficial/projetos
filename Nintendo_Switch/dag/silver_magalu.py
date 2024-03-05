from bs4 import BeautifulSoup
import re
import json
from google.cloud import storage

# Lista para armazenar os dados extraídos
list_todos = []

def silver_data_magalu():

    bucket_name = ""
    folder_name = ""
    file_name = ""

    # Inicialize o cliente do Cloud Storage
    storage_client = storage.Client.from_service_account_json('')


    # Acesse o bucket
    bucket = storage_client.bucket(bucket_name)

    # Obtenha o blob (objeto) do arquivo HTML
    blob = bucket.blob(folder_name+file_name)

    # Leia o conteúdo do blob (arquivo HTML)
    content = blob.download_as_string().decode("utf-8")

    # Parseie o HTML usando BeautifulSoup
    sopa_bonita = BeautifulSoup(content, 'html.parser')

    # Encontre todos os elementos relevantes na página
    list_titulo = sopa_bonita.find_all('h2', {'data-testid': 'product-title'})
    list_preco_promo = sopa_bonita.find_all('p', {'data-testid': 'price-value'})
    list_condition_promo = sopa_bonita.find_all('span', {'data-testid': 'in-cash'})
    list_parcelamento = sopa_bonita.find_all('p', {'data-testid': 'installment'})
    img_tags = sopa_bonita.find_all('img')
    img_srcs = [img['src'] for img in img_tags]

    for titulo, preco_promo, condition_promo, parcelado, img in zip(list_titulo, list_preco_promo, list_condition_promo, list_parcelamento, img_srcs):
        # Extraia o texto de cada elemento e faça algumas limpezas
        titulo = titulo.text.strip()
        moeda = preco_promo.text.strip()
        moeda = moeda[0] + moeda[1]
        preco_promo = preco_promo.text.strip().replace('R$', '').replace('\xa0', '').replace('.', '').replace(',', '.')
        condition_promo = condition_promo.text.strip()
        
        # Use expressão regular para extrair o valor do parcelamento
        parcelado_text = re.search(r'\d+\.\d+', parcelado.text)
        parcelado = parcelado_text.group() if parcelado_text else ''

        imagem = str(img).replace('[', '').replace(']', '')

        # Adicione os atributos à lista 'list_todos' como um dicionário
        list_todos.append({
            'titulo': titulo,
            'moeda': moeda,
            'preco_promo': preco_promo,
            'condition_promo': condition_promo,
            'parcelado': parcelado,
            'imagem': imagem
        })

    # Salvar o arquivo JSON no Google Cloud Storage
    silver = ""
    file_name_silver = ""
    json_data = json.dumps(list_todos)

    # Credenciais de autenticação do Google Cloud Storage
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(silver + file_name_silver)  # Aqui é onde especificamos o caminho do arquivo dentro da pasta
    blob.upload_from_string(json_data)

    return sopa_bonita, list_todos

# Chame a função get_data() e obtenha o status do request e a lista 'list_todos'
sopa_bonita, list_todos = silver_data_magalu()

# Verifique se a lista 'sopa_bonita' não está vazia
assert sopa_bonita, "A lista 'sopa_bonita' está vazia"
# Verifique se a lista 'list_todos' não está vazia
assert list_todos, "A lista 'list_todos' está vazia"