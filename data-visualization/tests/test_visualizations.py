import os
import sys
import pytest
from pyspark.sql import SparkSession

# add the parent directory to the Python module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from visualizations import (
    load_data, load_data_from_kafka, load_predicted_outbreaks,
    plot_event_type_distribution, plot_severity_distribution, plot_location_heatmap,
    plot_anomaly_distribution, plot_streaming_events, plot_past_outbreaks,
    plot_predicted_outbreak, plot_event_severity_correlation, plot_monthly_event_counts,
    plot_daily_event_counts, plot_event_location_correlation, plot_severity_trend,
    plot_unique_locations, plot_event_duration_distribution
)

@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .appName("HealthEventVisualizationTest") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_load_data():
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    assert data is not None
    assert not data.empty

def test_load_data_from_kafka(spark_session):
    streaming_data = load_data_from_kafka()
    assert streaming_data is not None
    assert streaming_data.isStreaming

def test_load_predicted_outbreaks(spark_session):
    predicted_outbreaks = load_predicted_outbreaks()
    assert predicted_outbreaks is not None
    assert isinstance(predicted_outbreaks, spark_session.sql.dataframe.DataFrame)

def test_plot_event_type_distribution(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_event_type_distribution(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('event_type_distribution.png').check()

def test_plot_severity_distribution(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_severity_distribution(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('severity_distribution.png').check()

def test_plot_location_heatmap(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_location_heatmap(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('location_heatmap.png').check()

def test_plot_anomaly_distribution(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_anomaly_distribution(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('anomaly_distribution.png').check()

def test_plot_streaming_events(spark_session, tmpdir):
    streaming_data = load_data_from_kafka()
    plot_streaming_events(streaming_data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('streaming_events.png').check()

def test_plot_past_outbreaks(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_past_outbreaks(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('past_outbreaks.png').check()

def test_plot_predicted_outbreak(spark_session, tmpdir):
    predicted_outbreaks = load_predicted_outbreaks()
    plot_predicted_outbreak(predicted_outbreaks)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('predicted_outbreak.png').check()

def test_plot_event_severity_correlation(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_event_severity_correlation(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('event_severity_correlation.png').check()

def test_plot_monthly_event_counts(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_monthly_event_counts(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('monthly_event_counts.png').check()

def test_plot_daily_event_counts(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_daily_event_counts(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('daily_event_counts.png').check()

def test_plot_event_location_correlation(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_event_location_correlation(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('event_location_correlation.png').check()

def test_plot_severity_trend(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_severity_trend(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('severity_trend.png').check()

def test_plot_unique_locations(spark_session, tmpdir):
    streaming_data = load_data_from_kafka()
    plot_unique_locations(streaming_data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('unique_locations.png').check()

def test_plot_event_duration_distribution(tmpdir):
    data = load_data('../spark-analytics/data/1m_health_events_dataset.csv')
    plot_event_duration_distribution(data)
    assert len(tmpdir.listdir()) == 1
    assert tmpdir.join('event_duration_distribution.png').check()