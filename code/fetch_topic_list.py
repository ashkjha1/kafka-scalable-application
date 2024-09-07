from confluent_kafka.admin import AdminClient
import os
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("SERVER")


def list_kafka_topics(bootstrap_servers=SERVER):

    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    try:
        # Fetch the metadata of the cluster, including the topics
        metadata = admin_client.list_topics(timeout=10)
        topics = metadata.topics.keys()
        return f"Available topics: {list(topics)}"
    except Exception as e:
        return f"Failed to list topics: {e}"


if __name__ == "__main__":
    # Example usage
    result = list_kafka_topics()
    print(result)
