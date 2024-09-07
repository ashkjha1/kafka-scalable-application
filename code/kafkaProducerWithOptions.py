from confluent_kafka import Producer
from data_generator import generate_message
import json
import random
import os 
from dotenv import load_dotenv
load_dotenv()

def delivery_report(err, msg):
    """ Called once for each message produced. """
    if err is not None:
        print(f"Delivery failed for message key {msg.key()}: {err}")
    else:
        print(f"Message produced: {msg.value()} to partition {msg.partition()}")

def produce_messages(bootstrap_servers, topic):
    producer_config = {
    'bootstrap.servers': bootstrap_servers,  # Kafka broker(s)
    'acks': 'all',  # Ensure that the leader and all in-sync replicas acknowledge the message
    'compression.type': 'gzip',  # Use gzip compression for messages
    }
    producer = Producer(producer_config)

    # Asynchronous producer
    try:
        for i in range(100):
            message=generate_message()
            encoded_message=json.dumps(message).encode('utf-8')
            key=json.dumps(random.randint(1,10)).encode('utf-8')
            # producer.produce(topic, key=key, value=encoded_message, callback=delivery_report, partition=0)

            ## comment: Partition argument need not be passed or all partition should be defined incase of multiple partition setup
            producer.produce(topic, key=key, value=encoded_message, callback=delivery_report)

        # Block for up to 1 second for events. Callbacks will be invoked during
        # this method call if the message was acknowledged.
        producer.poll(10000)
    finally:
        # Wait for any outstanding messages to be delivered and then close the producer
        producer.flush()

if __name__ == '__main__':
    bootstrap_servers = os.getenv('SERVER')  # Replace with your Kafka broker(s)
    TOPIC = 'kafka.learnings.orders'
    produce_messages(bootstrap_servers, TOPIC)