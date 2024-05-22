# anomaly_detection.py
# pyright: reportAttributeAccessIssue=false

import pathlib
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

import anomaly_detection_trainer # import will cause the code to run

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "health_events"

# create a SparkSession
spark = SparkSession.builder \
    .appName("AnomalyDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

# load the trained model
model_path = pathlib.Path().cwd() / "models" / "anomaly_detection_model"
loaded_model = PipelineModel.load(str(model_path))

# define the schema for the incoming streaming data
schema = StructType([
    StructField("EventType", StringType(), True),
    StructField("Timestamp", TimestampType(), True),
    StructField("Location", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("Details", StringType(), True)
])

# create a streaming DataFrame from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# parse the JSON data from Kafka
parsed_df = kafka_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# make predictions on the streaming data
predictions = loaded_model.transform(parsed_df)

# select the relevant columns and rename the prediction column
output_df = predictions.select("EventType", "Timestamp", "Location", "Severity", "Details", "prediction") \
    .withColumnRenamed("prediction", "Is_Anomaly")

# save the predicted anomalies as a Parquet file
output_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "../data-visualization/data/predicted_anomalies") \
    .option("checkpointLocation", "checkpoints/predicted_anomalies") \
    .start()

# write the predictions to the console
query = output_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

spark.stop()
