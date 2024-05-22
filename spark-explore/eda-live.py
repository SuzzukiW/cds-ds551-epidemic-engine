import os
import sys
import urllib.request
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, date_format, hour, dayofweek, month, year

# add the current directory to the Python path
sys.path.append(os.getcwd())

# find the spark installation
spark_home = findspark.init()

# download the kafka package JAR file if not already present
jar_dir = os.path.join(os.getcwd(), "jars")
os.makedirs(jar_dir, exist_ok=True)
jar_path = os.path.join(jar_dir, "spark-sql-kafka-0-10_2.12-3.2.1.jar")

if not os.path.exists(jar_path):
    kafka_package_url = "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.2.1.jar"
    print(f"Downloading {kafka_package_url}")
    urllib.request.urlretrieve(kafka_package_url, jar_path)
    print("Download complete.")

# create a Spark session with the Kafka package in the classpath
jar_paths = [jar_path]
spark = SparkSession.builder \
    .appName("HealthEventsEDA") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .config("spark.jars", ",".join(jar_paths)) \
    .getOrCreate()

# define the schema for the health event data
schema = StructType([
    StructField("EventType", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("Details", StringType(), True)
])

# kafka configuration
kafka_topics = ["hospital_admission", "emergency_incident", "vaccination", "low", "medium", "high", "general_events"]
kafka_server = "44.201.154.178:9092"

# create a streaming DataFrame to read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", ",".join(kafka_topics)) \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# convert Timestamp column to timestamp data type
df = df.withColumn("Timestamp", to_timestamp("Timestamp", "yyyy-MM-dd HH:mm:ss"))

# extract additional date and time features
df = df.withColumn("Date", date_format("Timestamp", "yyyy-MM-dd")) \
        .withColumn("Hour", hour("Timestamp")) \
        .withColumn("DayOfWeek", dayofweek("Timestamp")) \
        .withColumn("Month", month("Timestamp")) \
        .withColumn("Year", year("Timestamp"))

# perform exploratory data analysis on the streaming data
# event frequency by event type
event_freq = df.groupBy("EventType").count()

# event frequency by location
location_freq = df.groupBy("Location").count().orderBy("count", ascending=False)

# event frequency by severity
severity_freq = df.groupBy("Severity").count().orderBy("count", ascending=False)

# event frequency over time (windowed)
windowedCounts = df \
    .groupBy(window("Timestamp", "1 hour"), "EventType") \
    .count() \
    .orderBy("window")

# event frequency by event type and location
event_type_location_freq = df.groupBy("EventType", "Location").count().orderBy("count", ascending=False)

# event frequency by event type and severity
event_type_severity_freq = df.groupBy("EventType", "Severity").count().orderBy("count", ascending=False)

# event frequency by location and severity
location_severity_freq = df.groupBy("Location", "Severity").count().orderBy("count", ascending=False)

# event frequency by hour of the day
hourly_freq = df.groupBy("Hour").count().orderBy("Hour")

# event frequency by day of the week
day_freq = df.groupBy("DayOfWeek").count().orderBy("DayOfWeek")

# event frequency by month
monthly_freq = df.groupBy("Month").count().orderBy("Month")

# event frequency by year
yearly_freq = df.groupBy("Year").count().orderBy("Year")

# start the streaming query for event frequency
query_event_freq = event_freq \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# print the EDA results
print("Event frequency by event type:")
query_event_freq.awaitTermination()

print("Event frequency by location:")
location_freq.show(truncate=False)

print("Event frequency by severity:")
severity_freq.show(truncate=False)

print("Event frequency over time (windowed):")
windowedCounts.show(truncate=False)

print("Event frequency by event type and location:")
event_type_location_freq.show(truncate=False)

print("Event frequency by event type and severity:")
event_type_severity_freq.show(truncate=False)

print("Event frequency by location and severity:")
location_severity_freq.show(truncate=False)

print("Event frequency by hour of the day:")
hourly_freq.show(truncate=False)

print("Event frequency by day of the week:")
day_freq.show(truncate=False)

print("Event frequency by month:")
monthly_freq.show(truncate=False)

print("Event frequency by year:")
yearly_freq.show(truncate=False)

query.awaitTermination()