import json
from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
load_dotenv()

def consume_messages(bootstrap_servers, topic, group_id):
    consumer_config={
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
    }
    consumer = Consumer(consumer_config)

    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
            else:
                print(f"Consumed message: {msg.value().decode('utf-8')}")
                # print("-------------------------------")
                # print(f"Error: {msg.error()}")
                # print(f"Headers: {msg.headers()}")
                # # print(f"Key: {msg.key().decode()}")
                # print(f"Latency: {msg.latency()}")
                # print(f"Leader Epoch: {msg.leader_epoch()}")
                # print(f"Offset: {msg.offset()}")
                # print(f"Partition: {msg.partition()}")
                # # If you have custom attributes, access them here
                # # print(f"Set Headers: {msg.set_headers}")  # Replace with correct access
                # # print(f"Set Key: {msg.set_key}")        # Replace with correct access
                # # print(f"Set Value: {msg.set_value}")    # Replace with correct access
                # print(f"Timestamp: {msg.timestamp()}")
                # print(f"Topic: {msg.topic()}")
                # print(f"Value: {msg.value().decode()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__=="__main__":
    bootstrap_servers = os.getenv('SERVER')

    bootstrap_servers = bootstrap_servers
    TOPIC = 'kafka.learnings.orders'
    group_id = "kafka-python-consumers"
    consume_messages(bootstrap_servers, TOPIC, group_id)
