import pandas as pd
from confluent_kafka import Consumer, Producer
import json


producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_conf)

def lire_et_traiter_csv(fichier_csv):
    df = pd.read_csv(fichier_csv,encoding='ISO-8859-1')
    df = df[(df['TERRITORY'].notna()) & (df['ADDRESSLINE2'].notna()) & (df["POSTALCODE"].notna()) & (df["STATE"].notna())]
    df = df[df['QUANTITYORDERED'] > 30]
    df = df.drop(['CONTACTFIRSTNAME', 'MSRP', 'PRODUCTCODE'], axis=1)
    df['id'] = range(1, len(df) + 1)
    return df

def produire_message_kafka(topic, df):
    for index, row in df.iterrows():
        message = row.to_dict()
        json_data = json.dumps(message)
        producer.produce(topic, value=json_data)
        producer.flush()

def main():
    df=lire_et_traiter_csv('source-data/sales_data_sample.csv')
    produire_message_kafka("sale_data",df)

if __name__ == '__main__':
    main()