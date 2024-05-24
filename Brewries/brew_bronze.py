# codigo_execucao.py
import requests
import json
import os

def get_data_from_api():
    base_url = 'https://api.openbrewerydb.org/breweries'
    response = requests.get(base_url)
    data = response.json()
    return response.status_code, data

def save_data_to_file(data):
    path = r'Brew\data\bronze'
    filename = os.path.join(path, 'breweries.json')
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return filename


