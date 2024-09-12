import json

from confluent_kafka import Consumer, KafkaException, KafkaError
import psycopg2
from psycopg2 import sql
from datetime import datetime

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Adresse du serveur Kafka
    'group.id': 'mon_groupe_consommateur',   # ID du groupe de consommateurs
    'auto.offset.reset': 'earliest',         # Commencer à lire depuis le début des logs
    'enable.auto.commit': True               # Commit automatique des offsets
}

consumer = Consumer(consumer_conf)

topics = ['sale_data']
consumer.subscribe(topics)

db_config = {
    'host': 'localhost',
    'port': 5432,
    'database': 'sale_data',
    'user': 'postgres',
    'password': 'python3.3.3',
}

def format_date(date_str):
    # Convertit la date au format 'MM/DD/YYYY HH:MM' en 'YYYY-MM-DD HH:MM:SS'
    try:
        date_obj = datetime.strptime(date_str, '%m/%d/%Y %H:%M')
        return date_obj.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None

def table_exists(conn, table_name):
    cursor = conn.cursor()
    query = sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)")
    cursor.execute(query, (table_name,))
    exists = cursor.fetchone()[0]
    cursor.close()
    return exists


def create_table_if_not_exists():
    conn = psycopg2.connect(**db_config)
    if not table_exists(conn, 'sales_data'):
        create_table_query = """
        CREATE TABLE sales_data (
            ORDERNUMBER INTEGER NOT NULL,
            QUANTITYORDERED INTEGER NOT NULL,
            PRICEEACH DECIMAL(10, 2) NOT NULL,
            ORDERLINENUMBER INTEGER NOT NULL,
            SALES DECIMAL(10, 2) NOT NULL,
            ORDERDATE TIMESTAMP NOT NULL,
            STATUS VARCHAR(50) NOT NULL,
            QTR_ID INTEGER NOT NULL,
            MONTH_ID INTEGER NOT NULL,
            YEAR_ID INTEGER NOT NULL,
            PRODUCTLINE VARCHAR(50) NOT NULL,
            CUSTOMERNAME VARCHAR(100) NOT NULL,
            PHONE VARCHAR(20),
            ADDRESSLINE1 VARCHAR(100),
            ADDRESSLINE2 VARCHAR(100),
            CITY VARCHAR(50),
            STATE VARCHAR(50),
            POSTALCODE VARCHAR(10),
            COUNTRY VARCHAR(50),
            TERRITORY VARCHAR(50),
            CONTACTLASTNAME VARCHAR(50),
            DEALSIZE VARCHAR(50),
            PRIMARY KEY (ORDERNUMBER, ORDERLINENUMBER)
        );
        """
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
    conn.close()

def insert_into_db(data):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO sales_data (
        ORDERNUMBER, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER, SALES,
        ORDERDATE, STATUS, QTR_ID, MONTH_ID, YEAR_ID, PRODUCTLINE,
        CUSTOMERNAME, PHONE, ADDRESSLINE1, ADDRESSLINE2, CITY, STATE,
        POSTALCODE, COUNTRY, TERRITORY, CONTACTLASTNAME, DEALSIZE
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Assurez-vous que toutes les valeurs sont présentes et formatées correctement
    values = (
        data.get('ORDERNUMBER'),
        data.get('QUANTITYORDERED'),
        data.get('PRICEEACH'),
        data.get('ORDERLINENUMBER'),
        data.get('SALES'),
        format_date(data.get('ORDERDATE')),  # Convertir la date au format correct
        data.get('STATUS'),
        data.get('QTR_ID'),
        data.get('MONTH_ID'),
        data.get('YEAR_ID'),
        data.get('PRODUCTLINE'),
        data.get('CUSTOMERNAME'),
        data.get('PHONE'),
        data.get('ADDRESSLINE1'),
        data.get('ADDRESSLINE2'),
        data.get('CITY'),
        data.get('STATE'),
        data.get('POSTALCODE'),
        data.get('COUNTRY'),
        data.get('TERRITORY'),
        data.get('CONTACTLASTNAME'),
        data.get('DEALSIZE')
    )

    print(values)
    cursor.execute(insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()

def main():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            data = json.loads(msg.value().decode('utf-8'))
            # Insérer les données dans la base de données
            insert_into_db(data)

    except KeyboardInterrupt:
        print("Arrêt du consommateur.")
    finally:
        consumer.close()

if __name__ == '__main__':
    create_table_if_not_exists()
    main()