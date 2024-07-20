from google.cloud import storage
import pandas as pd
import re
from io import StringIO

def get_gcs_bucket():
    bucket_name = "fifa-datalake"

    client = storage.Client.from_service_account_json('/home/fececa/airflow/google-credencial/buckets-project-427021-50f0526fd3e5.json')
    
    bucket = client.get_bucket(bucket_name)
    
    return bucket

def push_blob_names(bucket, prefix):

    blobs = bucket.list_blobs(prefix=prefix)

    blob_names = [blob.name for blob in blobs]

    if blob_names:
        print(f'A lista de arquivos contém: {len(blob_names)}')
    else:
        print('A lista de arquivos retornou vazia')

    return blob_names

def read_blobs(bucket, blob_names):

    data_frames = []
    
    for blob_name in blob_names:
        
        if blob_name.endswith('.json'):
         
            blob = bucket.blob(blob_name)
         
            content = blob.download_as_text()
         
            df = pd.read_json(StringIO(content))
         
            df['File Name'] = blob_name
         
            data_frames.append(df)

    json_objects = pd.concat(data_frames, ignore_index=True)

    return json_objects

def process_data(json_objects):

    df = json_objects.explode('pagination', ignore_index=True)

    df = df.explode('items', ignore_index=True)

    df = pd.concat([df.drop(['pagination', 'items'], axis=1), 
                    df['pagination'].apply(pd.Series), 
                    df['items'].apply(pd.Series)], axis=1)
    
    df['File Date'] = df['File Name'].apply(lambda x: re.search(r'\d{4}-\d{2}-\d{2}', x).group(0))
    
    df = df.sort_values(by='File Date', ascending=False)

    df = df.drop_duplicates(subset=['id'], keep='first')

    return df

def save_df_to_gcs(df, bucket, directory_path, file_name):

    csv_buffer = StringIO()

    df.to_csv(csv_buffer, index=False)

    blob = bucket.blob(f'{directory_path}/{file_name}')
    
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
