from kafka import KafkaConsumer, TopicPartition
import json
import re
import pandas as pd


def clean_json_string(raw_value):
    json_str = re.sub(r"[\r\n]+", "", raw_value)  # Supprimer les nouvelles lignes
    json_str = re.sub(r"(?<!\\)'", '"', json_str)  # Convertir les guillemets simples en guillemets doubles
    json_str = re.sub(r"nan", "null", json_str, flags=re.IGNORECASE)  # Convertir nan en null
    json_str = re.sub(r"None", "null", json_str)  # Convertir None en null
    return json_str


# Création du consommateur Kafka
consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    group_id='my-consumer-group',
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
            # Nettoyer et décoder le message
            raw_value = msg.value.decode('utf-8')

            dict_from_value = json.loads(raw_value)
            # Ajouter à la liste si nécessaire
            data.append(dict_from_value)

        except (ValueError, SyntaxError) as e:
            print(f"Erreur lors de la conversion en dictionnaire : {e}")
        except Exception as e:
            print(f"Erreur lors du traitement du message : {e}")

finally:
    consumer.close()
    print("Consommateur fermé proprement.")


# Traitement et affichage des données combinées, si nécessaire
