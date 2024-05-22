# test_consumer.py
from unittest.mock import call

def test_consume_messages_and_produce(mock_kafka_consumer, mock_kafka_producer, mock_db_connection):
    # setup
    from consumer import consume_messages_and_produce
    
    # mock consumer to read a list of messages
    mock_consumer_instance = mock_kafka_consumer.return_value.__enter__.return_value
    mock_consumer_instance.__iter__.return_value = [{
        'value': {'EventType': 'hospital_admission', 'Severity': 'high', 'Timestamp': '2021-07-21T12:34:56', 'Location': 'Hospital A', 'Details': 'Condition X'}
    }]
    
    # call the function
    consume_messages_and_produce()

    # check that the message was produced to the correct topic
    expected_topic = 'high'
    mock_producer_instance = mock_kafka_producer.return_value.__enter__.return_value
    assert mock_producer_instance.send.called
    assert mock_producer_instance.send.call_args == call(expected_topic, {
        'EventType': 'hospital_admission', 'Severity': 'high', 'Timestamp': '2021-07-21T12:34:56', 'Location': 'Hospital A', 'Details': 'Condition X'
    })

    # check that the message was logged to the database
    mock_db_instance = mock_db_connection.return_value
    assert mock_db_instance.cursor.called
    assert mock_db_instance.commit.called

"""Running Tests
You can run your tests using pytest from the terminal:

pytest kafka-server/tests/

"""
