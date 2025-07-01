import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError
from tenacity import retry, stop_after_attempt, wait_fixed


def is_fully_ready(metadata):
    return metadata.brokers \
        and all(broker.id is not None for broker in metadata.brokers.values())


# Confirm server access by fetching cluster metadata
@retry(stop=stop_after_attempt(10), wait=wait_fixed(3))
def wait_for_kafka(admin_client: AdminClient):
    print("Checking Kafka availability...")
    metadata = admin_client.list_topics(timeout=5)
    if not is_fully_ready(metadata):
        raise KafkaException("Not all brokers are available yet")
    print("Kafka is fully available")


def create_topic(topic: str, kafka_cluster: str):
    # configuration
    conf = {
        "bootstrap.servers": kafka_cluster
    }

# Create topic
    admin_client = AdminClient(conf)

    try:
        wait_for_kafka(admin_client)
    except KafkaException as e:
        print(f"Kafka not ready: {e}")
        exit(1)

    new_topic = NewTopic(topic, num_partitions=2, replication_factor=2)
    fs = admin_client.create_topics(new_topics=[new_topic])

    # Poll repeatedly until all futures are done or timeout exceeded
    timeout = 30
    start = time.time()

    while True:
        admin_client.poll(1)
        if all(f.done() for f in fs.values()):
            break
        if time.time() - start > timeout:
            print("Timeout waiting for topic creation")
            break

    for topic, f in fs.items():
        try:
            f.result()  # will raise if creation failed
            print(f"Topic '{topic}' created successfully")
        except KafkaError as e:
            print(f"Failed to create topic '{topic}': {e}")
            # Optionally retry here
