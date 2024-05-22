# visualizations.py
# pyright: reportAttributeAccessIssue=false

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from flask import Flask, render_template
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_format, window, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

app = Flask(__name__, template_folder='templates', static_folder='static')

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "health_events"

def load_data(file_path):
    data = pd.read_csv(file_path)
    return data

def load_data_from_kafka():
    spark = SparkSession.builder \
        .appName("HealthEventVisualization") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
        .getOrCreate()

    schema = StructType([
        StructField("EventType", StringType(), True),
        StructField("Timestamp", TimestampType(), True),
        StructField("Location", StringType(), True),
        StructField("Severity", StringType(), True),
        StructField("Details", StringType(), True),
        StructField("Is_Anomaly", IntegerType(), True)
    ])

    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    parsed_df = kafka_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    return parsed_df

def load_predicted_outbreaks():
    spark = SparkSession.builder \
        .appName("HealthEventVisualization") \
        .getOrCreate()

    predicted_outbreaks = spark.read.parquet("data/predicted_outbreaks")

    return predicted_outbreaks

def load_predicted_anomalies():
    spark = SparkSession.builder \
        .appName("HealthEventVisualization") \
        .getOrCreate()

    predicted_anomalies = spark.read.parquet("data/predicted_anomalies")

    return predicted_anomalies

def plot_event_type_distribution(data):
    plt.figure(figsize=(10, 6))
    event_type_counts = data['EventType'].value_counts()
    plt.pie(event_type_counts, labels=event_type_counts.index, autopct='%1.1f%%')
    plt.title('Distribution of Event Types')
    plt.savefig('static/event_type_distribution.png')
    plt.close()

def plot_severity_distribution(data):
    plt.figure(figsize=(10, 6))
    severity_counts = data['Severity'].value_counts()
    plt.bar(severity_counts.index, severity_counts.values)
    plt.xlabel('Severity')
    plt.ylabel('Count')
    plt.title('Distribution of Severity Levels')
    plt.savefig('static/severity_distribution.png')
    plt.close()

def plot_location_heatmap(data):
    location_counts = data['Location'].value_counts()
    location_df = pd.DataFrame({'Location': location_counts.index, 'Count': location_counts.values})
    plt.figure(figsize=(10, 6))
    sns.heatmap(location_df.pivot_table(index='Location', values='Count'), cmap='viridis', annot=True, fmt='0.1f')
    plt.title('Heatmap of Event Counts by Location')
    plt.savefig('static/location_heatmap.png')
    plt.close()

def plot_anomaly_distribution(data):
    plt.figure(figsize=(8, 6))
    anomaly_counts = data['Is_Anomaly'].value_counts()
    labels = ['Non-Anomaly' if label == 0 else 'Anomaly' for label in anomaly_counts.index]
    plt.pie(anomaly_counts, labels=labels, autopct='%1.1f%%')
    plt.title('Distribution of Anomalies')
    plt.savefig('static/anomaly_distribution.png')
    plt.close()

def plot_streaming_events(streaming_data):
    query = streaming_data \
        .withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("Date", date_format(col("Timestamp"), "yyyy-MM-dd")) \
        .groupBy(window(col("Timestamp"), "1 hour"), col("EventType")) \
        .count() \
        .orderBy(col("window").asc()) \
        .writeStream \
        .queryName("streaming_events_view") \
        .outputMode("complete") \
        .format("memory") \
        .start()

    query.awaitTermination(timeout=10)

    event_counts_pd = query.status['message']

    plt.figure(figsize=(12, 8))
    for event_type in event_counts_pd['EventType'].unique():
        event_type_data = event_counts_pd[event_counts_pd['EventType'] == event_type]
        plt.plot(event_type_data['window'].apply(lambda x: x.start), event_type_data['count'], label=event_type)

    plt.xlabel('Time')
    plt.ylabel('Event Count')
    plt.title('Streaming Event Counts by Type')
    plt.legend()
    plt.savefig('static/streaming_events.png')
    plt.close()

    query.stop()

def plot_past_outbreaks(data, persisted_data):
    # Combine the provided dataset and persisted streaming data
    combined_data = pd.concat([data, persisted_data])
    
    combined_data['Timestamp'] = pd.to_datetime(combined_data['Timestamp'])
    combined_data['Date'] = combined_data['Timestamp'].dt.date

    outbreak_data = combined_data[combined_data['Is_Anomaly'] == 1]
    outbreak_counts = outbreak_data.groupby(['Date', 'Location']).size().reset_index(name='Count') # type: ignore

    plt.figure(figsize=(12, 8))
    sns.lineplot(data=outbreak_counts, x='Date', y='Count', hue='Location')
    plt.xlabel('Date')
    plt.ylabel('Outbreak Count')
    plt.title('Past Outbreaks by Location')
    plt.savefig('static/past_outbreaks.png')
    plt.close()

def plot_predicted_outbreak(predicted_outbreaks):
    predicted_outbreaks_pd = predicted_outbreaks.toPandas()

    plt.figure(figsize=(10, 6))
    sns.countplot(data=predicted_outbreaks_pd, x='Location', hue='Severity')
    plt.xlabel('Location')
    plt.ylabel('Count')
    plt.title('Predicted Outbreaks by Location and Severity')
    plt.savefig('static/predicted_outbreak.png')
    plt.close()

def plot_predicted_anomalies(predicted_anomalies):
    predicted_anomalies_pd = predicted_anomalies.toPandas()

    plt.figure(figsize=(10, 6))
    sns.countplot(data=predicted_anomalies_pd, x='Location', hue='EventType')
    plt.xlabel('Location')
    plt.ylabel('Count')
    plt.title('Predicted Anomalies by Location and Event Type')
    plt.savefig('static/predicted_anomalies.png')
    plt.close()

def plot_event_severity_correlation(data):
    event_severity_counts = data.groupby(['EventType', 'Severity']).size().reset_index(name='Count')
    event_severity_pivot = event_severity_counts.pivot(index='EventType', columns='Severity', values='Count')

    plt.figure(figsize=(12, 8))
    sns.heatmap(event_severity_pivot, cmap='coolwarm', annot=True, fmt='0.1f')
    plt.title('Correlation between Event Types and Severity Levels')
    plt.savefig('static/event_severity_correlation.png')
    plt.close()

def plot_monthly_event_counts(data):
    data['Timestamp'] = pd.to_datetime(data['Timestamp'])
    data['Month'] = data['Timestamp'].dt.to_period('M')

    monthly_counts = data.groupby(['Month', 'EventType']).size().reset_index(name='Count')

    plt.figure(figsize=(12, 8))
    sns.lineplot(data=monthly_counts, x='Month', y='Count', hue='EventType')
    plt.xlabel('Month')
    plt.ylabel('Event Count')
    plt.title('Monthly Event Counts by Type')
    plt.savefig('static/monthly_event_counts.png')
    plt.close()

def plot_daily_event_counts(data):
    data['Timestamp'] = pd.to_datetime(data['Timestamp'])
    data['Date'] = data['Timestamp'].dt.date

    daily_counts = data.groupby(['Date', 'EventType']).size().reset_index(name='Count')

    plt.figure(figsize=(12, 8))
    sns.lineplot(data=daily_counts, x='Date', y='Count', hue='EventType')
    plt.xlabel('Date')
    plt.ylabel('Event Count')
    plt.title('Daily Event Counts by Type')
    plt.xticks(rotation=45)
    plt.savefig('static/daily_event_counts.png')
    plt.close()

def plot_event_location_correlation(data):
    event_location_counts = data.groupby(['EventType', 'Location']).size().reset_index(name='Count')
    event_location_pivot = event_location_counts.pivot(index='EventType', columns='Location', values='Count')

    plt.figure(figsize=(12, 8))
    sns.heatmap(event_location_pivot, cmap='YlGnBu', annot=True, fmt='d')
    plt.title('Correlation between Event Types and Locations')
    plt.savefig('static/event_location_correlation.png')
    plt.close()

def plot_severity_trend(data):
    data['Timestamp'] = pd.to_datetime(data['Timestamp'])
    data['Date'] = data['Timestamp'].dt.date

    severity_trend = data.groupby(['Date', 'Severity']).size().reset_index(name='Count')

    plt.figure(figsize=(12, 8))
    sns.lineplot(data=severity_trend, x='Date', y='Count', hue='Severity')
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.title('Severity Trend over Time')
    plt.xticks(rotation=45)
    plt.savefig('static/severity_trend.png')
    plt.close()

def plot_unique_locations(streaming_data):
    unique_locations = streaming_data \
        .groupBy(window(col("Timestamp"), "1 hour")) \
        .agg(countDistinct("Location").alias("Unique_Locations")) \
        .orderBy(col("window").asc())

    unique_locations_pd = unique_locations.toPandas()

    plt.figure(figsize=(12, 8))
    plt.plot(unique_locations_pd['window'].apply(lambda x: x.start), unique_locations_pd['Unique_Locations'])
    plt.xlabel('Time')
    plt.ylabel('Unique Locations')
    plt.title('Number of Unique Locations over Time')
    plt.savefig('static/unique_locations.png')
    plt.close()

def plot_event_duration_distribution(data):
    data['Timestamp'] = pd.to_datetime(data['Timestamp'])
    data['Date'] = data['Timestamp'].dt.date

    event_duration = data.groupby(['EventType', 'Date']).size().reset_index(name='Duration')

    plt.figure(figsize=(12, 8))
    sns.boxplot(data=event_duration, x='EventType', y='Duration')
    plt.xlabel('Event Type')
    plt.ylabel('Duration (Days)')
    plt.title('Distribution of Event Duration by Type')
    plt.xticks(rotation=45)
    plt.savefig('static/event_duration_distribution.png')
    plt.close()

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    loaded_data = load_data('data/1m_health_events_dataset.csv')
    # streaming_data = load_data_from_kafka()
    # predicted_outbreaks = load_predicted_outbreaks()
    # predicted_anomalies = load_predicted_anomalies()

    plot_event_type_distribution(loaded_data)
    plot_severity_distribution(loaded_data)
    plot_location_heatmap(loaded_data)
    plot_anomaly_distribution(loaded_data)
    # plot_streaming_events(streaming_data)
    # plot_past_outbreaks(loaded_data, predicted_anomalies)
    # plot_predicted_outbreak(predicted_outbreaks)
    # plot_predicted_anomalies(predicted_anomalies)
    plot_event_severity_correlation(loaded_data)
    # plot_monthly_event_counts(loaded_data)
    plot_daily_event_counts(loaded_data)
    plot_event_location_correlation(loaded_data)
    plot_severity_trend(loaded_data)
    # plot_unique_locations(streaming_data)
    plot_event_duration_distribution(loaded_data)
    print('Running app')
    app.run(host='0.0.0.0', debug=True)
