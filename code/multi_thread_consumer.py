import threading
import logging
from confluent_kafka import Consumer, KafkaError
import os
from dotenv import load_dotenv
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')

# Function to consume messages from Kafka
def consume_messages(consumer_config, topic_name):
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic_name])

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll messages with a timeout of 1 second
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f"End of partition reached {msg.partition()}")
                else:
                    logging.error(msg.error())
            else:
                # Process the message
                logging.info(f"Consumed message: {msg.value().decode('utf-8')} from partition: {msg.partition()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Kafka configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'consumer-test-group',
    'auto.offset.reset': 'earliest'
}

topic_name = os.getenv('TOPIC')
num_threads = 4  # Number of consumer threads

# Start consumer threads
threads = []
for i in range(num_threads):
    thread = threading.Thread(target=consume_messages, args=(consumer_config, topic_name))
    thread.start()
    threads.append(thread)

# Join threads
for thread in threads:
    thread.join()

