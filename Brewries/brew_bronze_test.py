# test_codigo_execucao.py
import os
import pytest
import brew_bronze

def test_api_response():
    status_code, data = brew_bronze.get_data_from_api()
    assert status_code == 200
    assert data is not None

def test_file_creation():
    data = {"teste": "dados"}
    filename = brew_bronze.save_data_to_file(data)
    assert os.path.exists(filename)


