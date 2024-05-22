# conftest.py
import pytest
from unittest.mock import MagicMock, patch

@pytest.fixture
def mock_kafka_consumer():
    with patch('consumer.KafkaConsumer') as MockConsumer:
        yield MockConsumer()

@pytest.fixture
def mock_kafka_producer():
    with patch('consumer.KafkaProducer') as MockProducer:
        yield MockProducer()

@pytest.fixture
def mock_db_connection():
    with patch('sqlite3.connect') as mock_connect:
        mock_connect.return_value.cursor.return_value.execute = MagicMock()
        yield mock_connect
