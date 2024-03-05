from bs4 import BeautifulSoup
import json
from google.cloud import storage

# Lista para armazenar os dados extraídos
list_todos = []

def silver_data_ml():

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

    list_titulo = sopa_bonita.find_all('h1', {'class': 'ui-pdp-title'})
    list_moeda = sopa_bonita.find_all('span', {'class': 'andes-money-amount__currency-symbol'})
    list_preco_promo = sopa_bonita.find_all('span', {'class': 'andes-money-amount__fraction'})
    list_condition_promo = sopa_bonita.find_all('span', {'class': 'andes-money-amount__discount'})
    img_tags = sopa_bonita.find_all('img', class_='ui-pdp-image ui-pdp-gallery__figure__image')
    img_srcs = [img.get('src') for img in img_tags]
    list_parcela_green = sopa_bonita.find_all('p', {'class': 'ui-pdp-color--GREEN ui-pdp-size--MEDIUM ui-pdp-family--REGULAR'})
    list_parcela_black = sopa_bonita.find_all('p', {'class': 'ui-pdp-color--BLACK ui-pdp-size--MEDIUM ui-pdp-family--REGULAR'})
    list_parcelado = list_parcela_green + list_parcela_black


    for titulo, moeda, preco_promo,condition_promo, img, parcelado in zip(list_titulo, list_moeda, list_preco_promo,list_condition_promo, img_srcs,list_parcelado):
        # Extraia o texto de cada elemento e faça algumas limpezas
        titulo_text = titulo.text.strip()
        moeda = moeda.text.strip()
        preco_promo_text = preco_promo.text.strip().replace('\xa0','').replace('.','').replace(',','.')
        condition_promo_text = condition_promo.text.strip()
        imagem = str(img).replace('[', '').replace(']', '')
        parcelado_text = parcelado.text.strip()    

        # Remover caracteres \n e espaços extras dos valores das chaves
        titulo_text = titulo_text.replace('\n', '').strip()
        preco_promo_text = preco_promo_text.replace('\n', '').strip()
        condition_promo_text = condition_promo_text.replace('\n', '').strip()
        parcelado_text = parcelado_text.replace('\n', '').strip()


        # Adicione os atributos à lista 'list_todos' como um dicionário
        list_todos.append({
            'titulo': titulo_text,
            'moeda': moeda,
            'preco_promo': preco_promo_text,
            'condition_promo': condition_promo_text,
            'parcelado': parcelado_text,
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
sopa_bonita, list_todos = silver_data_ml()

# Verifique se a lista 'sopa_bonita' não está vazia
assert sopa_bonita, "A lista 'sopa_bonita' está vazia"
# Verifique se a lista 'list_todos' não está vazia
assert list_todos, "A lista 'list_todos' está vazia"