import requests
import pandas as pd
from google.cloud import storage, error_reporting

def upload_file_to_gcs():

    blob_name = "desafio4.csv"

    # Definindo client para publicar errors no Cloud Monitoring
    client = error_reporting.Client()

    # Definindo informações sobre o bucket
    bucket_name = "my_new_bucket"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket with name {bucket.name} has been created")

    # Carregando dados da API
    api_url = "https://jsonplaceholder.typicode.com/todos"
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()
    else:
        client.report("Request ERROR:", response.status_code)

    df = pd.DataFrame(data)

    bucket.blob(blob_name).upload_from_string(df.to_csv(index=False), 'text/csv')
