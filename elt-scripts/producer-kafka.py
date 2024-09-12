import pandas as pd
from kafka import KafkaProducer
import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.4.1")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION
)


def read_csv_and_send_to_kafka(file_path,topic):
    df = pd.read_csv(file_path,encoding='ISO-8859-1')
    for index,row in df.iterrows():
        message=str(row.to_dict()).encode('utf-8')
        producer.send(topic, message)


read_csv_and_send_to_kafka("source-data/sales_data_sample.csv","sales_raw_data")