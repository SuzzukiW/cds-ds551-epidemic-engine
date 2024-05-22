# test_model_trainer.py

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from pyspark.sql import SparkSession
from model_trainer import preprocess_data, create_pipeline, train_pipeline, evaluate_model, retrain_model, make_predictions

from pyspark.sql.types import LongType

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("TestModelTrainer").getOrCreate()
    yield spark
    spark.stop()

def test_preprocess_data(spark):
    # create sample data
    data = spark.createDataFrame([
        ("Event1", "2023-05-01 10:00:00", "Location1", "High", "Details1", "1"),
        ("Event2", "2023-05-02 11:00:00", "Location2", "Low", "Details2", "0")
    ], ["EventType", "Timestamp", "Location", "Severity", "Details", "Is_Anomaly"])
    
    # call the preprocess_data function
    preprocessed_data = preprocess_data(data)
    
    # assert the expected columns and data types
    assert set(preprocessed_data.columns) == {"EventType", "Timestamp", "Location", "Severity", "Details", "Is_Anomaly", "days_since_event", "hour", "day_of_week", "month"}
    assert isinstance(preprocessed_data.schema["Timestamp"].dataType, LongType)

def test_create_pipeline():
    # call the create_pipeline function
    pipeline = create_pipeline()
    
    # assert the expected number of stages in the pipeline
    assert len(pipeline.getStages()) == 9

# and we may need to add more tests here!