# test_consumer_postgres.py

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from unittest.mock import patch, MagicMock
from consumer_postgres import connect_to_postgres, create_consumer, push_db, main

def test_connect_to_postgres_failure():
    # ensure the connection attempt raises an exception when psycopg2.connect fails
    with patch('consumer_postgres.psycopg2.connect', side_effect=Exception("connection failed")):
        with pytest.raises(Exception, match="connection failed"):
            connect_to_postgres()

def test_create_consumer(mocker):
    mocker.patch('consumer_postgres.KafkaConsumer', return_value=MagicMock())
    consumer = create_consumer()
    assert isinstance(consumer, MagicMock)  # checking against MagicMock because KafkaConsumer is mocked

@patch('consumer_postgres.create_consumer', return_value=MagicMock())
@patch('consumer_postgres.connect_to_postgres', return_value=MagicMock())
@patch('consumer_postgres.push_db')

def test_main(push_db_mock, connect_to_postgres_mock, create_consumer_mock):
    consumer_mock = MagicMock()
    consumer_mock.__iter__.return_value = iter([MagicMock(value={'some': 'data'})])
    create_consumer_mock.return_value = consumer_mock

    main()

    push_db_mock.assert_called_once()