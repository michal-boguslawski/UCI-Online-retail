import os

from packages.create_topic import KafkaHandler
from packages.helper_functions import download_and_extract_file
from packages.produce import KafkaProducer


if __name__ == "__main__":
    # get env variables
    kafka_cluster = os.getenv("KAFKA_CLUSTERS")
    kafka_topic = os.getenv("KAFKA_TOPIC")
    schema_registry = os.getenv("SCHEMA_REGISTRY")

    # get data
    url = 'https://archive.ics.uci.edu/static/public/352/online+retail.zip'
    file_paths = download_and_extract_file(url)

    # create topic
    handler = KafkaHandler(bootstrap_servers=kafka_cluster)
    handler._kafka_set_connection()
    if handler._check_kafka_connection():
        handler.create_kafka_topics(kafka_topic)

    # start producing to kafka
    kafka_producer = KafkaProducer(
        schema_registry=schema_registry,
        bootstrap_servers=kafka_cluster
    )

    kafka_producer.run(
        topic=kafka_topic,
        file_path=file_paths[0]
    )
