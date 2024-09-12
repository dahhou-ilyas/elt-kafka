from kafka import KafkaConsumer, TopicPartition
import json
import re
import sys


def clean_json_string(raw_value):
    # Retirer les retours à la ligne et les espaces inutiles
    json_str = re.sub(r"[\r\n]+", " ", raw_value)

    # Remplacer les apostrophes non échappées par des guillemets
    json_str = re.sub(r"(?<!\\)'", '"', json_str)

    # Remplacer les occurrences de 'nan' et 'None' par 'null'
    json_str = re.sub(r"nan", "null", json_str, flags=re.IGNORECASE)
    json_str = re.sub(r"None", "null", json_str)

    return json_str


consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    group_id='my-consumer-group',
    enable_auto_commit=False,
    auto_offset_reset='earliest'
)

partition = TopicPartition('sales_raw_data', 0)
consumer.assign([partition])
data=[]
try:
    for msg in consumer:
        try:
            raw_value = msg.value.decode('utf-8')
            json_str = clean_json_string(raw_value)
            dict_from_value = json.loads(json_str)
            data.append(dict_from_value)
            consumer.commit()

        except (ValueError, SyntaxError) as e:
            print(f"Erreur lors de la conversion en dictionnaire : {e}")
        except Exception as e:
            print(f"Erreur lors du traitement du message : {e}")


except KeyboardInterrupt:
    print("Arrêt du consommateur.")
finally:
    # Nettoyage et fermeture du consommateur
    consumer.close()
    print("Consommateur fermé proprement.")