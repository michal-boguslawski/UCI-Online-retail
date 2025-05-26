from confluent_kafka.admin import AdminClient, NewTopic

# configuration
conf = {
    "bootstrap.servers": "kafka-1:9092, kafka-2:9092"
}

topic = "sales_events"

# Create topic
admin_client = AdminClient(conf)
new_topic = NewTopic(topic, num_partitions=2, replication_factor=2)
fs = admin_client.create_topics(new_topics=[new_topic])

# Wait for operation to finish, test creation of topic
for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic '{topic}' created")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")
