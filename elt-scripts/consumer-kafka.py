from kafka import KafkaConsumer, TopicPartition
import json
import re
import pandas as pd

def clean_json_string(raw_value):
    json_str = re.sub(r"[\r\n]+", "", raw_value)

    json_str = re.sub(r"(?<!\\)'", '"', json_str)

    json_str = re.sub(r"nan", "null", json_str, flags=re.IGNORECASE)
    json_str = re.sub(r"None", "null", json_str)

    return json_str

# Création du consommateur Kafka
consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    group_id='my-consumer-group',  # Assurez-vous d'utiliser un group_id unique si nécessaire
    enable_auto_commit=False,
    auto_offset_reset='latest'
)

# Assignation de la partition
partition = TopicPartition('sales_raw_data', 0)
consumer.assign([partition])

data = []

try:
    for msg in consumer:
        try:
            raw_value = msg.value.decode('utf-8')
            json_str = clean_json_string(raw_value)
            dict_from_value = json.loads(json_str)
            df = pd.DataFrame([dict_from_value])
            df_cleaned = df.dropna(subset=['TERRITORY'])
            print(df_cleaned)
        except (ValueError, SyntaxError) as e:
            print(f"Erreur lors de la conversion en dictionnaire : {e}")
        except Exception as e:
            print(f"Erreur lors du traitement du message : {e}")

finally:
    consumer.close()
    print("Consommateur fermé proprement.")
