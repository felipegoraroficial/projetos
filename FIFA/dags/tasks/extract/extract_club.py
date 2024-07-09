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
def obter_total_paginas_clube(**kwargs):
    url = "https://futdb.app/api/clubs?page=1"  # API endpoint to get the first page of clubs
    try:
        response = requests.get(url, headers=headers)  # Make a GET request to the API
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()  # Parse the JSON response
        page_total = data['pagination']['pageTotal']  # Extract the total number of pages
        print(f"Número total de páginas: {page_total}")  # Print the total number of pages
        kwargs['ti'].xcom_push(key='page_total_clube', value=page_total)  # Push the total pages to XCom for use by other tasks
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter o número total de páginas: {e}")  # Print error message if the request fails
        kwargs['ti'].xcom_push(key='page_total_clube', value=1)  # Push a default value of 1 to XCom in case of error

# Function to get the Google Cloud Storage bucket
def get_gcs_bucket():
    bucket_name = "fifa-datalake"  # Define the bucket name
    # Create a Google Cloud Storage client using a service account JSON file
    client = storage.Client.from_service_account_json('/home/fececa/airflow/google-credencial/buckets-project-427021-50f0526fd3e5.json')
    bucket = client.get_bucket(bucket_name)  # Get the bucket object
    return bucket  # Return the bucket object

# Function to collect club data and store it in Google Cloud Storage
def clube(**kwargs):
    folder_name = "clube/raw/"  # Define the folder name for storing raw club data
    file_name = f"clube_{current_date}.json"  # Define the file name with the current date

    # Retrieve the total number of pages from XCom
    page_total = kwargs['ti'].xcom_pull(key='page_total_clube', task_ids='total_club_pags')
    if page_total is None:
        page_total = 1  # Set a default value if the XCom pull fails

    clube_list = []  # Initialize an empty list to store club data

    # Loop through each page to collect club data
    for i in range(1, page_total + 1):
        url = f"https://futdb.app/api/clubs?page={i}"  # API endpoint for each page
        try:
            response = requests.get(url, headers=headers)  # Make a GET request to the API
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()  # Parse the JSON response
            clube_list.append(data)  # Append the club list
        except requests.exceptions.RequestException as e:
            print(f"Erro ao obter dados da página {i}: {e}")  # Print error message if the request fails

    bucket = get_gcs_bucket()  # Get the Google Cloud Storage bucket
    blob = bucket.blob(folder_name + file_name)  # Create a new blob object in the specified folder
    # Upload the club data to the blob as a JSON string
    blob.upload_from_string(data=json.dumps(clube_list), content_type='application/json')
    print(f"Lista de clubes carregados com o nome: {file_name} no diretório: {folder_name}")  # Print success message
    kwargs['ti'].xcom_push(key='clube_list', value=clube_list)  # Push the club list to XCom for use by other tasks

# Function to collect and store club images in Google Cloud Storage
def imagem_clube(**kwargs):
    bucket = get_gcs_bucket()  # Get the Google Cloud Storage bucket

    # Retrieve the club list from XCom
    clube_list = kwargs['ti'].xcom_pull(key='clube_list', task_ids='club_data')
    if clube_list is None:
        print("Nenhuma lista de clubes encontrada.")  # Print message if no club list is found
        return

    clube_imagens = []  # Initialize an empty list to store club images metadata

    # Loop through each club to collect their images
    for club_item in clube_list:
        items = club_item['items'] # get the items in json file
        for club_info in items:
            club_id = club_info['id']  # Get the club ID
            club_name = club_info['name']  # Get the club name
            url_imagem = f"https://futdb.app/api/clubs/{club_id}/image"  # API endpoint for the club image
            response = requests.get(url_imagem, headers=headers)  # Make a GET request to the API

            if response.status_code == 200:
                blob_name = f"clube/imagens/{club_id}.jpg"  # Define the blob name for the club image
                blob = bucket.blob(blob_name)  # Create a new blob object
                blob.upload_from_string(response.content, content_type='image/jpeg')  # Upload the image to the blob
                # Append metadata about the image to the club images list
                clube_imagens.append({'ID do Clube': club_id, 'Nome do Clube': club_name, 'URL do Blob': blob.public_url})
                print(f"Imagem de {club_name} salva no Google Cloud Storage no diretório 'clube/imagens/'")  # Print success message
            else:
                print(f"Erro ao acessar a API para {club_name}: {response.status_code}")  # Print error message if the request fails

    return clube_imagens  # Return the list of club images metadata

