""" Consume messages from the 'health_events' topic, categorize them based on event type and severity, and produce them to the appropriate topics using separate Kafka producers. """
import json
import time
import logging
import sqlite3
from kafka import KafkaConsumer, KafkaProducer # type: ignore

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_producer(bootstrap_servers):
    """
    Create a Kafka producer instance.

    Args:
        bootstrap_servers (list): List of Kafka bootstrap servers.

    Returns:
        KafkaProducer: Kafka producer instance.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def connect_consumer(source):
    """ Connect to the Kafka consumer. """
    for tries in range(10):
        try:
            consumer = KafkaConsumer(
                'health_events',
                bootstrap_servers=[source],
                auto_offset_reset='earliest',
                group_id='health_event_consumer',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            break
        except Exception as e:
            logger.error(f"Error creating Kafka consumer: {e}")
            time.sleep(3)

        if tries >= 9:
            raise Exception("Failed to create Kafka consumer")

    return consumer

def consume_messages_and_produce(default_source='44.201.154.178:9092'):
    """
    Consume messages from the 'health_events' topic, categorize them based on event type 
    and severity, and produce them to the appropriate topics using separate Kafka producers.
    """
    # Create a Kafka consumer instance
    consumer = connect_consumer(default_source)

    # Create db connection
    con = connect_db('health_events.db')

    # Create Kafka producer instances for each topic
    producers = {
        'hospital_admission': create_producer([default_source]),
        'emergency_incident': create_producer([default_source]),
        'vaccination': create_producer([default_source]),
        'low': create_producer([default_source]),
        'medium': create_producer([default_source]),
        'high': create_producer([default_source]),
        'other': create_producer([default_source])
    }

    # Mapping of event types and severities to their corresponding topics here
    topic_mapping = {
        'hospital_admission': 'hospital_admission',
        'emergency_incident': 'emergency_incident',
        'vaccination': 'vaccination',
        'low': 'low',
        'medium': 'medium',
        'high': 'high',
        'other': 'general_events'
    }

    # Consume messages from the 'health_events' topic
    for message in consumer:
        try:
            # Parse the message value as JSON
            data = message.value

            # Extract the event type and severity from the message
            event_type = data.get('EventType', 'other').lower()
            severity = data.get('Severity', 'low').lower()

            # Determine the target topic based on event type and severity
            target_topic = topic_mapping.get(event_type, topic_mapping.get(severity, 'general_events'))

            # Get the producer instance for the target topic
            producer = producers[target_topic]

            # Send the message to the target topic
            producer.send(target_topic, data)

            # Log the sent message and target topic
            logger.info(f"Sent message to {target_topic}: {data}")

            push_db(data, con)

        except Exception as e:
            # Log any errors that occur during message processing
            logger.error(f"Error processing message: {e}")

    # Close the consumer and producers
    consumer.close()
    for producer in producers.values():
        producer.close()

def connect_db(db_name='health_events.db'):
    """
    Connect to the SQLite database.

    Returns:
        sqlite3.Connection: SQLite database connection.
    """

    for tries in range(10):
        try:
            connection = sqlite3.connect(db_name)
            break
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            time.sleep(3)

        if tries >= 9:
            raise Exception("Failed to connect to database")

    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS health_events (\
            event_type TEXT, \
            timestamp TEXT, \
            location TEXT, \
            severity TEXT, \
            details TEXT \
        )")

    return connection

def push_db(data, connection):
    """
    Push the data to the database.

    Args:
        data (dict): Data to be pushed to the database.
    """

    logger.info(data)
    # Insert the data into the 'health_events' table
    with connection:
        connection.execute("INSERT INTO health_events ( \
            event_type, timestamp, location, severity, details \
        ) VALUES (:EventType, :Timestamp, :Location, :Severity, :Details)", data)

    # Log the inserted data
    logger.info(f"Inserted data into database: {data}")

if __name__ == "__main__":
    consume_messages_and_produce()
