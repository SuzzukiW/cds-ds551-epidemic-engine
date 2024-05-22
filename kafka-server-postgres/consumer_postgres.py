# consumer_postgres.py

import time
import json
import psycopg2
import logging
from kafka import KafkaConsumer # type: ignore

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_postgres():
    """Create a persistent PostgreSQL connection and ensure the table exists."""
    try:
        connection = psycopg2.connect(
            dbname='health_events',
            user='admin',
            password='secret',
            host='postgres',
            connect_timeout=10
        )
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS health_events (
                event_type TEXT,
                timestamp TIMESTAMP,
                location TEXT,
                severity TEXT,
                details TEXT
            );
        """)
        connection.commit()
        cursor.close()
        logging.info("Connected to PostgreSQL and verified table exists.")
        return connection
    except psycopg2.OperationalError as error:
        logging.error(f"Failed to connect to PostgreSQL: {error}")
        raise


def create_consumer():
    """Create and return a Kafka consumer."""
    consumer = KafkaConsumer(
        'health_events',
        bootstrap_servers=['kafka:9093'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logging.info("Kafka consumer created.")
    return consumer

def push_db(data, connection):
    """Push the data to the PostgreSQL database using a context manager for automatic commit/rollback."""
    try:
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO health_events (event_type, timestamp, location, severity, details)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (data['event_type'], data['timestamp'], data['location'], data['severity'], data['details']))
        logging.info("Data successfully inserted into PostgreSQL.")
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        raise

def main():
    db_connection = connect_to_postgres()
    consumer = create_consumer()

    try:
        for message in consumer:
            push_db(message.value, db_connection)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        if db_connection:
            db_connection.rollback()
    finally:
        if db_connection:
            db_connection.close()
        consumer.close()

if __name__ == "__main__":
    main()
