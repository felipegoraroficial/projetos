import requests
from bs4 import BeautifulSoup
from google.cloud import storage

headers = {'user-agent': 'Mozilla/5.0'}

# Lista para armazenar o conteúdo HTML de cada página
html_pages = []

def get_data_magalu():

    for page_number in range(1, 17):
        # Faça uma requisição GET para cada página de resultados
        resposta = requests.get(f"https://www.magazineluiza.com.br/busca/nintendo+switch/?page={page_number}", headers=headers)
        sopa = BeautifulSoup(resposta.text, 'html.parser')
        html_pages.append(sopa.prettify())

    # Salvar o arquivo de texto no Google Cloud Storage
    bucket_name = ""
    folder_name = ""
    file_name = ""  # Mudança no nome do arquivo para .txt

    # Credenciais de autenticação do Google Cloud Storage
    client = storage.Client.from_service_account_json('')
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(folder_name + file_name)  # Aqui é onde especificamos o caminho do arquivo dentro da pasta

    # Fazendo upload do conteúdo HTML para o blob
    # Concatenando todas as páginas HTML em uma única string
    all_pages_text = '\n'.join(html_pages)
    blob.upload_from_string(all_pages_text, content_type='text/plain; charset=utf-8')  # Alteração do tipo de conteúdo para texto

    return resposta.status_code, html_pages

# Chame a função get_data() e obtenha o status do request e a lista 'html_pages'
status_code, html_pages = get_data_magalu()

# Verifique se o status do request é 200
assert status_code == 200, "O status do request não é 200"
# Verifique se a lista 'html_pages' não está vazia
assert html_pages, "A lista 'html_pages' está vazia"


