import os
import time
import random
import json
from sys import api_version

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TEST = os.environ.get("KAFKA_TOPIC_TEST", "test")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.4.1")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION,
)

i = 0
while i <= 30:
    producer.send(
        KAFKA_TOPIC_TEST,
        json.dumps({"message": f"xxxxxxxxxx! - test {i}"}).encode("utf-8"),
    )
    i += 1
producer.flush()

