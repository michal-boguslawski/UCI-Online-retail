from confluent_kafka.admin import NewTopic
from .helper_functions import KafkaConnector


class KafkaHandler(KafkaConnector):
    def __init__(self, bootstrap_servers: str):
        super().__init__(bootstrap_servers=bootstrap_servers)

    def create_kafka_topics(
        self,
        new_topics: list[str] | str,
        num_partitions: int = 2,
        replication_factor: int = 2,
    ) -> bool:
        """
        Create new Kafka topics in an idempotent way.

        This method checks for existing topics in the Kafka cluster and
        only creates the ones that do not already exist. It uses the
        `AdminClient.create_topics` API and handles asynchronous results.

        Args:
            new_topics (list[str] | str):
                A list of topic names to create or a topic name.
            num_partitions (int, optional):
                Number of partitions for each topic. Defaults to 2.
            replication_factor (int, optional):
                Replication factor for each topic. Defaults to 2.

        Returns:
            bool:
                True if the operation succeeded (topics created or already existed),
                False if the Kafka connection is not established.

        Notes:
            - If a topic already exists, it is skipped.
            - If all topics already exist, no changes are made.
            - Exceptions raised during topic creation are caught and printed.
        """
        if isinstance(new_topics, str):
            new_topics = [new_topics]

        if self.kafka_admin_client is None:
            print("Kafka connection is not established")
            return False

        # Fetch current topics
        existing_topics = set(
            self.kafka_admin_client.list_topics(timeout=5).topics.keys()
        )

        # Filter only new ones
        topics_to_create = [
            NewTopic(
                topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            for topic in new_topics
            if topic not in existing_topics
        ]

        if not topics_to_create:
            print("All topics already exist, nothing to create")
            return True

        # Fire off async create requests
        fs = self.kafka_admin_client.create_topics(new_topics=topics_to_create)

        # Collect results
        for topic, f in fs.items():
            try:
                f.result()  # returns None if success
                print(f"Topic '{topic}' created successfully")
            except Exception as e:
                print(f"Failed to create topic '{topic}': {e}")

        return True


if __name__ == "__main__":
    handler = KafkaHandler("localhost:29092,localhost:29093")
    handler._kafka_set_connection()
    if handler._check_kafka_connection():
        handler.create_kafka_topics(
            ["topic1", "topic2"],
            num_partitions=2,
            replication_factor=2
        )
