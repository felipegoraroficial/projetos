# Import necessary libraries
import requests  # For making HTTP requests
import json  # For handling JSON data
import datetime  # For handling date and time
from google.cloud import storage  # For interacting with Google Cloud Storage

# Define the API key and headers for the request
api_key = "d9e52390-62a7-46e1-9ac2-3a69df5f3324"
headers = {"X-AUTH-TOKEN": api_key}

# Get the current date in the format YYYY-MM-DD
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Function to obtain the total number of pages from the API
def obter_total_paginas_liga(**kwargs):
    url = "https://futdb.app/api/leagues?page=1"  # API endpoint to get the first page of leagues
    try:
        response = requests.get(url, headers=headers)  # Make a GET request to the API
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()  # Parse the JSON response
        page_total = data['pagination']['pageTotal']  # Extract the total number of pages
        print(f"Número total de páginas: {page_total}")  # Print the total number of pages
        kwargs['ti'].xcom_push(key='page_total_liga', value=page_total)  # Push the total pages to XCom for use by other tasks
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter o número total de páginas: {e}")  # Print error message if the request fails
        kwargs['ti'].xcom_push(key='page_total_liga', value=1)  # Push a default value of 1 to XCom in case of error

# Function to get the Google Cloud Storage bucket
def get_gcs_bucket():
    bucket_name = "fifa-datalake"  # Define the bucket name
    # Create a Google Cloud Storage client using a service account JSON file
    client = storage.Client.from_service_account_json('/home/fececa/airflow/google-credencial/buckets-project-427021-50f0526fd3e5.json')
    bucket = client.get_bucket(bucket_name)  # Get the bucket object
    return bucket  # Return the bucket object

# Function to collect league data and store it in Google Cloud Storage
def liga(**kwargs):
    folder_name = "liga/raw/"  # Define the folder name for storing raw league data
    file_name = f"liga_{current_date}.json"  # Define the file name with the current date

    # Retrieve the total number of pages from XCom
    page_total = kwargs['ti'].xcom_pull(key='page_total_liga', task_ids='total_league_pags')
    if page_total is None:
        page_total = 1  # Set a default value if the XCom pull fails

    liga_list = []  # Initialize an empty list to store league data

    # Loop through each page to collect league data
    for i in range(1, page_total + 1):
        url = f"https://futdb.app/api/leagues?page={i}"  # API endpoint for each page
        try:
            response = requests.get(url, headers=headers)  # Make a GET request to the API
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()  # Parse the JSON response
            liga_list.append(data)  # Append the club list
        except requests.exceptions.RequestException as e:
            print(f"Erro ao obter dados da página {i}: {e}")  # Print error message if the request fails

    bucket = get_gcs_bucket()  # Get the Google Cloud Storage bucket
    blob = bucket.blob(folder_name + file_name)  # Create a new blob object in the specified folder
    # Upload the league data to the blob as a JSON string
    blob.upload_from_string(data=json.dumps(liga_list), content_type='application/json')
    print(f"Lista de ligas carregados com o nome: {file_name} no diretório: {folder_name}")  # Print success message
    kwargs['ti'].xcom_push(key='liga_list', value=liga_list)  # Push the league list to XCom for use by other tasks

# Function to collect and store league images in Google Cloud Storage
def imagem_liga(**kwargs):
    bucket = get_gcs_bucket()  # Get the Google Cloud Storage bucket

    # Retrieve the league list from XCom
    liga_list = kwargs['ti'].xcom_pull(key='liga_list', task_ids='league_data')
    if liga_list is None:
        print("Nenhuma lista de ligas encontrada.")  # Print message if no league list is found
        return

    liga_imagens = []  # Initialize an empty list to store league images metadata

    # Loop through each league to collect their images
    for league_item in liga_list:
        items = league_item['items'] # get the items in json file
        for league_info in items:
            league_id = league_info['id']  # Get the league ID
            league_name = league_info['name']  # Get the league name
            url_imagem = f"https://futdb.app/api/leagues/{league_id}/image"  # API endpoint for the league image
            response = requests.get(url_imagem, headers=headers)  # Make a GET request to the API

            if response.status_code == 200:
                blob_name = f"liga/imagens/{league_id}.jpg"  # Define the blob name for the league image
                blob = bucket.blob(blob_name)  # Create a new blob object
                blob.upload_from_string(response.content, content_type='image/jpeg')  # Upload the image to the blob
                # Append metadata about the image to the league images list
                liga_imagens.append({'ID do liga': league_id, 'Nome do liga': league_name, 'URL do Blob': blob.public_url})
                print(f"Imagem de {league_name} salva no Google Cloud Storage no diretório 'liga/imagens/'")  # Print success message
            else:
                print(f"Erro ao acessar a API para {league_name}: {response.status_code}")  # Print error message if the request fails

    return liga_imagens  # Return the list of league images metadata

