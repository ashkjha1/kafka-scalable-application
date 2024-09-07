from confluent_kafka.admin import AdminClient
import os
from dotenv import load_dotenv
load_dotenv()

SERVER=os.getenv('SERVER')

def get_topic_details(topic_name, bootstrap_servers=SERVER):
    admin_client = AdminClient({
        "bootstrap.servers": bootstrap_servers
    })

    try:
        # Get metadata for the topic
        metadata = admin_client.list_topics(topic=topic_name, timeout=10)
        if topic_name in metadata.topics:
            topic_metadata = metadata.topics[topic_name]

            print(f"Topic: {topic_name}")
            print(f"Partitions: {len(topic_metadata.partitions)}")
            for partition_id, partition in topic_metadata.partitions.items():
                print(f"  Partition {partition_id}: Leader: {partition.leader}, Replicas: {partition.replicas}, ISR: {partition.isrs}")
        else:
            print(f"Topic '{topic_name}' does not exist")
    except Exception as e:
        print(f"Failed to get topic details: {e}")


if __name__=="__main__":
    topic=input('Enter the topic for which detail is required: ')
    get_topic_details(topic_name=topic)
