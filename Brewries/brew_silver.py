import requests
import json
import os

base_url = 'https://api.openbrewerydb.org/breweries'

response = requests.get(base_url)

data = response.json()

path = r'C:\Users\felip\OneDrive\Cursos e Certificados\Data Scientis\meus-scripts\Brew\data\bronze'
filename = os.path.join(path, 'breweries.json')

with open(filename, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

