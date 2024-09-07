from confluent_kafka.admin import AdminClient, NewTopic
import os
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("SERVER")
TOPIC = os.getenv("TOPIC")


def create_kafka_topic(
    topic_name,
    num_partitions=3,
    replication_factor=1,
    bootstrap_servers=SERVER,
):
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    topic_list = [
        NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
    ]
    fs = admin_client.create_topics(topic_list)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{topic}' created successfully")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")


if __name__ == "__main__":
    # Example usage
    create_kafka_topic(topic_name=TOPIC, num_partitions=3, replication_factor=2)
