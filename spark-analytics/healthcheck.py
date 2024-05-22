""" Healthcheck script for the spark and kafka services. """
import sys
import requests

def check_spark_master():
    """ Check if the spark master is up and running. """
    try:
        response = requests.get("http://spark-master:8080", timeout=10)
        if response.status_code == 200:
            return True
    except requests.exceptions.RequestException:
        pass
    return False

def check_kafka():
    """ Check if the kafka service is up and running. """
    try:
        response = requests.get("http://kafka:9092", timeout=10)
        if response.status_code == 200:
            return True
    except requests.exceptions.RequestException:
        pass
    return False

if __name__ == "__main__":
    if check_spark_master() and check_kafka():
        sys.exit(0)
    else:
        sys.exit(1)
