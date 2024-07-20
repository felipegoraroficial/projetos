from fifa.bronze.bronze_all import get_gcs_bucket, push_blob_names, read_blobs, process_data
import pickle

def identify_blobs(**kwargs):

    bucket = get_gcs_bucket()

    pais_df = push_blob_names(bucket, "país/raw/")
    liga_df = push_blob_names(bucket, "liga/raw/")
    clube_df = push_blob_names(bucket, "clube/raw/")
    
    blobs = {
        "pais_df": pais_df,
        "liga_df": liga_df,
        "clube_df": clube_df
    }
    with open('/tmp/blobs.pkl', 'wb') as f:
        pickle.dump(blobs, f)

def read_blob_data(**kwargs):

    with open('/tmp/blobs.pkl', 'rb') as f:
        blobs = pickle.load(f)
    
    bucket = get_gcs_bucket()

    pais = read_blobs(bucket, blobs['pais_df'])
    liga = read_blobs(bucket, blobs['liga_df'])
    clube = read_blobs(bucket, blobs['clube_df'])
    
    data = {
        "pais": pais,
        "liga": liga,
        "clube": clube
    }
    with open('/tmp/data.pkl', 'wb') as f:
        pickle.dump(data, f)

def process_blob_data(**kwargs):

    with open('/tmp/data.pkl', 'rb') as f:
        data = pickle.load(f)
    
    pais_final = process_data(data['pais'])
    liga_final = process_data(data['liga'])
    clube_final = process_data(data['clube'])
    
    processed_data = {
        "pais_final": pais_final,
        "liga_final": liga_final,
        "clube_final": clube_final
    }
    with open('/tmp/processed_data.pkl', 'wb') as f:
        pickle.dump(processed_data, f)