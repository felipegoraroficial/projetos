from bs4 import BeautifulSoup
import json
from google.cloud import storage

# Lista para armazenar os dados extraídos
list_todos = []

def silver_data_kabum():

    bucket_name = ""
    folder_name = ""
    file_name = ""
    file_name_imagens = ""

    # Inicialize o cliente do Cloud Storage
    storage_client = storage.Client.from_service_account_json('')

    def produtos_img():
        # Acesse o bucket
        bucket = storage_client.bucket(bucket_name)

        # Obtenha o blob (objeto) do arquivo HTML
        blob = bucket.blob(folder_name+file_name_imagens)

        # Leia o conteúdo do blob (arquivo HTML)
        content = blob.download_as_string().decode("utf-8")

        # Parseie o HTML usando BeautifulSoup
        sopa_bonita = BeautifulSoup(content, 'html.parser')

        return sopa_bonita
    produto_imagens = produtos_img()

    def produtos():
        # Acesse o bucket
        bucket = storage_client.bucket(bucket_name)

        # Obtenha o blob (objeto) do arquivo HTML
        blob = bucket.blob(folder_name+file_name)

        # Leia o conteúdo do blob (arquivo HTML)
        content = blob.download_as_string().decode("utf-8")

        # Parseie o HTML usando BeautifulSoup
        sopa_bonita = BeautifulSoup(content, 'html.parser')

        return sopa_bonita
    produtos_list = produtos()

    img_tags = produto_imagens.find_all('img', class_='imageCard')
    img_srcs = [img.get('src') for img in img_tags]

    # Encontre todos os elementos relevantes na página
    list_titulo = produtos_list.find_all('h1', {'class': 'sc-fdfabab6-6 jNQQeD'})
    list_preco_promo = produtos_list.find_all('h4', {'class': 'sc-5492faee-2 ipHrwP finalPrice'})
    list_condition_promo = produtos_list.find_all('span', {'class': 'sc-5492faee-3 igKOYC'})
    list_parcelamento = produtos_list.find_all('span', {'class': 'cardParcels'})

    for titulo, preco_promo, condition_promo, parcelado, img in zip(list_titulo, list_preco_promo, list_condition_promo, list_parcelamento, img_srcs):  # Modificação aqui
        # Extraia o texto de cada elemento e faça algumas limpezas
        titulo_text = titulo.text.strip() if titulo else ""
        moeda = preco_promo.text.strip()
        moeda = moeda[0] + moeda[1]
        preco_promo_text = preco_promo.text.strip().replace('R$','').replace('\xa0','').replace('.','').replace(',','.') if preco_promo else ""
        condition_promo_text = condition_promo.text.strip().replace('\xa0','') if condition_promo else ""
        
        parcelado_text = parcelado.text.strip().replace('R$','').replace('\xa0','').replace('.','').replace(',','.') if parcelado else ""
        imagem = str(img).replace('[', '').replace(']', '')

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

    return produtos_list, list_todos,produto_imagens

# Chame a função get_data() e obtenha o status do request e a lista 'list_todos'
produtos_list, list_todos, produto_imagens = silver_data_kabum()

# Verifique se a lista 'produto_imagens' não está vazia
assert produto_imagens, "A lista 'produto_imagens' está vazia"
# Verifique se a lista 'produtos_list' não está vazia
assert produtos_list, "A lista 'produtos_list' está vazia"
# Verifique se a lista 'list_todos' não está vazia
assert list_todos, "A lista 'list_todos' está vazia"