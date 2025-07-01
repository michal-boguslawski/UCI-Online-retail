import os, requests
from zipfile import ZipFile

from packages.create_topic import create_topic
from packages.produce import produce_to_kafka


if __name__ == "__main__":
    kafka_cluster = os.getenv("KAFKA_CLUSTERS")
    kafka_topic = os.getenv("KAFKA_TOPIC")
    schema_registry = os.getenv("SCHEMA_REGISTRY")
    
    # Step 1: Download the file
    url = 'https://archive.ics.uci.edu/static/public/352/online+retail.zip'
    zip_path = 'online+retail.zip'

    response = requests.get(url)
    with open(zip_path, 'wb') as f:
        f.write(response.content)

    # Step 2: Unzip the file
    with ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall('.')  # Extracts to the current directory
        
    print(os.listdir(), flush=True)
    file_name = os.path.abspath("Online Retail.xlsx")
    
    create_topic(topic=kafka_topic, kafka_cluster=kafka_cluster)
    produce_to_kafka(topic=kafka_topic, schema_registry=schema_registry, kafka_cluster=kafka_cluster, file_name=file_name)
    