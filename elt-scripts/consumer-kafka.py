from kafka import KafkaConsumer, TopicPartition

import pandas as pd

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',  # Important pour démarrer du début si l'offset n'est pas défini
    enable_auto_commit=False  # Désactiver l'auto-commit pour contrôler manuellement l'offset
)

partition = TopicPartition('sales_raw_data', 0)

consumer.assign([partition])



for msg in consumer:
    print(f"Offset: {msg.offset}, Key: {msg.key}, Value: {msg.value.decode('utf-8')}")