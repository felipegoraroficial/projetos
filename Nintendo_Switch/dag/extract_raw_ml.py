import requests
from bs4 import BeautifulSoup
from google.cloud import storage
import re

headers = {'user-agent': 'Mozilla/5.0'}

# Lista para armazenar o conteúdo HTML de cada página
html_pages = []

def get_data_ml():

    url = 'https://lista.mercadolivre.com.br/nintendo-sitwitch'
    resposta = requests.get(url, headers=headers)
    sopa_bonita = BeautifulSoup(resposta.text, 'html.parser')

    link_tags = sopa_bonita.find_all('a')
    filtered_links = [link for link in link_tags if 'nintendo switch' in link.get('title', '').lower() and not re.search(r'lista', link.get('href', '').lower())]

    # Iterar sobre os links filtrados
    for link_tag in filtered_links:
        # Obter o valor do atributo 'href' de cada link
        link = link_tag.get('href')

        resposta_produto = requests.get(link)
        sopa_produto = BeautifulSoup(resposta_produto.text, 'html.parser')
        html_pages.append(sopa_produto.prettify())

    # Salvar o arquivo JSON no Google Cloud Storage
    bucket_name = ""
    folder_name = ""
    file_name = "" 

    # Credenciais de autenticação do Google Cloud Storage
    client = storage.Client.from_service_account_json('')
    bucket = client.get_bucket(bucket_name)

    # Fazendo upload do conteúdo HTML para o blob
    # Concatenando todas as páginas HTML em uma única string
    all_pages_text = '\n'.join(html_pages)
    blob_html = bucket.blob(folder_name + file_name)
    blob_html.upload_from_string(all_pages_text, content_type='text/plain; charset=utf-8')

    return resposta.status_code, html_pages

# Chame a função get_data() e obtenha o status do request e a lista 'html_pages'
status_code, html_pages = get_data_ml()

# Verifique se o status do request é 200
assert status_code == 200, "O status do request não é 200"
# Verifique se a lista 'html_pages' não está vazia
assert html_pages, "A lista 'html_pages' está vazia"
