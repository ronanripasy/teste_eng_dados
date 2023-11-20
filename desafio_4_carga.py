from google.cloud import storage, bigtable
from google.cloud.bigtable import column_family

def main():
    # Dados da tabela no BigTable
    project_id="project-id"
    instance_id="instance-id"
    table_id="my-table"
    
    # Create a Cloud Bigtable client.
    client = bigtable.Client(project=project_id)
    # Connect to an existing Cloud Bigtable instance.
    instance = client.instance(instance_id)
    # Open an existing table.
    table = instance.table(table_id)

    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = "cf1"
    column_families = {column_family_id: max_versions_rule}
    
    if not table.exists():
        table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))

    # Dados bucket GCS
    bucket_name = "my_bucket_gcs"
    blob_name = "desafio4.csv"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        
        print(f.read())

        # Inserir dados do CSV na tabela do BigQuery, CloudSQL ou BigTable
        # Simulando a carga de dados na tabela do Big TAble
        row_key = "r1"
        row = table.read_row(row_key.encode("utf-8"))

        column_family_id = "cf1"
        column_id = "c1".encode("utf-8")
        value = row.cells[column_family_id][column_id][0].value.decode("utf-8")

        print("Row key: {}\nData: {}".format(row_key, value))

