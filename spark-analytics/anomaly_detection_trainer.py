# anomaly_detection_trainer.py
# pyright: reportAttributeAccessIssue=false, reportArgumentType=false

import pathlib
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# create a SparkSession
spark = SparkSession.builder \
    .appName("AnomalyDetectionTrain") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

# load the dataset
data_path = pathlib.Path().cwd() / "data" / "1m_health_events_dataset.csv"
df = spark.read.csv(str(data_path), header=True, inferSchema=True)


# select relevant columns for training
selected_columns = ["EventType", "Location", "Severity", "Is_Anomaly"]
df = df.select(selected_columns)

# convert categorical variables to numeric using StringIndexer
indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid="keep")
    for col in ["EventType", "Location", "Severity"]
]

# create a VectorAssembler to combine features into a single vector column
assembler = VectorAssembler(
    inputCols=["EventType_index", "Location_index", "Severity_index"],
    outputCol="features"
)

# split the data into training and testing sets
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# create a RandomForestClassifier
rf = RandomForestClassifier(labelCol="Is_Anomaly", featuresCol="features", numTrees=100)

# create a pipeline to chain the indexers, assembler, and classifier
pipeline = Pipeline(stages=indexers + [assembler, rf])

model = pipeline.fit(train_data)

model_path = pathlib.Path().cwd() / "models" / "anomaly_detection_model"
model.write().overwrite().save(str(model_path))
assert (model_path / 'metadata').is_dir()

predictions = model.transform(test_data)

# evaluate
evaluator = MulticlassClassificationEvaluator(labelCol="Is_Anomaly", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy:", accuracy)

# sample predictions
# predictions.select("EventType", "Location", "Severity", "Is_Anomaly", "prediction").show(100)

spark.stop()
