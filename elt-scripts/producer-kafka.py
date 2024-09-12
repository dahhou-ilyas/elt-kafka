from kafka import KafkaProducer
import os
import pandas as pd
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.4.1")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION
)

def on_send_success(record_metadata):
    print(f"Message envoyé à {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Erreur lors de l'envoi du message: {excp}")

def read_csv_and_send_to_kafka(file_path, topic):
    df = pd.read_csv(file_path, encoding='ISO-8859-1')
    i = 0
    for index, row in df.iterrows():
        message = str(row.to_dict()).encode('utf-8')
        print(f"Envoi du message: {message}")
        producer.send(topic, message).add_callback(on_send_success).add_errback(on_send_error)
        if i > 2:
            break
        i += 1
    producer.flush()

read_csv_and_send_to_kafka("source-data/sales_data_sample.csv", "sales_raw_data")
