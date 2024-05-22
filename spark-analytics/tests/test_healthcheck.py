# test_healthcheck.py

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import requests
from unittest.mock import patch
from healthcheck import check_spark_master, check_kafka

def test_check_spark_master():
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        assert check_spark_master() == True
        
        mock_get.return_value.status_code = 500
        assert check_spark_master() == False
        
        mock_get.side_effect = requests.exceptions.RequestException()
        assert check_spark_master() == False

def test_check_kafka():
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        assert check_kafka() == True
        
        mock_get.return_value.status_code = 500
        assert check_kafka() == False
        
        mock_get.side_effect = requests.exceptions.RequestException()
        assert check_kafka() == False