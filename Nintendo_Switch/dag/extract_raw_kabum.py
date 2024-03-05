import requests
from bs4 import BeautifulSoup
from google.cloud import storage

headers = {'user-agent': 'Mozilla/5.0'}

# Lista para armazenar o conteúdo HTML de cada página
html_pages = []
html_imagens = []

def get_data_kabum():

    # Loop para iterar sobre as páginas
    for page_number in range(1, 3):  # Neste exemplo, apenas uma página será verificada (de 1 a 2)
        # Faça uma requisição GET para cada página de resultados
        resposta = requests.get(f"https://www.kabum.com.br/gamer/nintendo/consoles-nintendo?page_number={page_number}&page_size=20&facet_filters=&sort=most_searched")
        sopa_bonita = BeautifulSoup(resposta.text, 'html.parser')
        html_imagens.append(sopa_bonita.prettify())

        img_tags = sopa_bonita.find_all('img', class_='imageCard')
        img_srcs = [img.get('src') for img in img_tags]

        elements = sopa_bonita.find_all('a', {'data-smarthintproductid': True})

        for element in elements:
            codigo = element['data-smarthintproductid']
            url_produto = f"https://www.kabum.com.br/produto/{codigo}"
            
            resposta_produto = requests.get(url_produto)
            sopa = BeautifulSoup(resposta_produto.text, 'html.parser')
            html_pages.append(sopa.prettify())

    # Salvar o arquivo JSON no Google Cloud Storage
    bucket_name = ""
    folder_name = ""
    file_name = "" 
    file_name_imagens = ""

    # Credenciais de autenticação do Google Cloud Storage
    client = storage.Client.from_service_account_json('')
    bucket = client.get_bucket(bucket_name)

    # Fazendo upload do conteúdo HTML para o blob
    # Concatenando todas as páginas HTML em uma única string
    all_pages_text = '\n'.join(html_pages)
    blob_html = bucket.blob(folder_name + file_name)
    blob_html.upload_from_string(all_pages_text, content_type='text/plain; charset=utf-8')

    # Convertendo a lista de elementos para uma string e fazendo upload
    elements_text = '\n'.join(map(str, html_imagens))
    blob_elements = bucket.blob(folder_name + file_name_imagens)
    blob_elements.upload_from_string(elements_text, content_type='text/plain; charset=utf-8')


    return resposta.status_code, html_pages, html_imagens

# Chame a função get_data() e obtenha o status do request e a lista 'html_pages'
status_code, html_pages, html_imagens = get_data_kabum()

# Verifique se o status do request é 200
assert status_code == 200, "O status do request não é 200"
# Verifique se a lista 'html_pages' não está vazia
assert html_pages, "A lista 'html_pages' está vazia"
# Verifique se a lista 'html_imagens' não está vazia
assert html_imagens, "A lista 'html_imagens' está vazia"