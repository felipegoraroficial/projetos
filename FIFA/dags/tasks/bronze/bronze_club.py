from google.cloud import storage
import pandas as pd
from io import StringIO
import re

def get_gcs_bucket():
    
    bucket_name = "fifa-datalake"
    
    client = storage.Client.from_service_account_json('/home/fececa/airflow/google-credencial/buckets-project-427021-50f0526fd3e5.json')
    
    bucket = client.get_bucket(bucket_name)
    
    return bucket

def list_blobs_with_prefix(**kwargs):

    bucket = get_gcs_bucket()
    
    folder_name = "clube/raw/" 

    blobs = bucket.list_blobs(prefix=folder_name
                              )
    blob_names = [blob.name for blob in blobs]

    if len(blob_names) > 0 :
        
        print(f'a lista de arquivos contem: {len(blob_names)}')

        kwargs['ti'].xcom_push(key = 'list_blobs', value = blob_names) 

    else:
        print('a lista de arquivos retornou vazia')

        kwargs['ti'].xcom_push(key = 'list_blobs', value =[ ]) 

def read_json_blobs(**kwargs):
    
    data_frames = []

    bucket = get_gcs_bucket()

    blob_names = kwargs['ti'].xcom_pull(key='list_blobs', task_ids='list_blobs_with_prefix')

    for blob_name in blob_names:

        if blob_name.endswith('.json'):

            blob = bucket.blob(blob_name)

            content = blob.download_as_text()

            df = pd.read_json(StringIO(content))

            df['File Name'] = blob_name

            data_frames.append(df)

    json_objects = pd.concat(data_frames, ignore_index=True)

    kwargs['ti'].xcom_push(key='json_blobs', value=json_objects) 

def process_json_data(**kwargs):

    json_objects = kwargs['ti'].xcom_pull(key='json_blobs', task_ids='read_json_blobs')
    
    df = json_objects.explode('pagination', ignore_index=True)

    df = df.explode('items', ignore_index=True)

    df = pd.concat([df.drop(['pagination', 'items'], axis=1), 
                    df['pagination'].apply(pd.Series), 
                    df['items'].apply(pd.Series)], axis=1)
    
    df['File Date'] = df['File Name'].apply(lambda x: re.search(r'\d{4}-\d{2}-\d{2}', x).group(0))
    
    df = df.sort_values(by='File Date', ascending=False)

    df = df.drop_duplicates(subset=['id'], keep='first')

    df = df[['File Name', 'File Date', 'id', 'name', 'league']]

    return df
