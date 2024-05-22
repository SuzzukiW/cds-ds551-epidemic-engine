""" Model Trainer for Health Risk Prediction System """
# model_trainer.py
# pyright: reportAttributeAccessIssue=false

import os
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, datediff, current_timestamp, hour, dayofweek, month, from_json, window, udf
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, MinMaxScaler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# constants
MODEL_DIR = "models"
LATEST_MODEL_DIR = "latest_model"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "health_events"
RETRAIN_INTERVAL = 3600  # Retrain every hour

# schema for the streaming data
schema = StructType([
    StructField("EventType", StringType(), True),
    StructField("Timestamp", TimestampType(), True),
    StructField("Location", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("Details", StringType(), True)
])

def preprocess_data(data):
    """
    Preprocess the input data by adding engineered features and converting timestamp to unix timestamp
    """
    preprocessed_data = data \
        .withColumn("days_since_event", datediff(current_timestamp(), col("Timestamp"))) \
        .withColumn("hour", hour(col("Timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("Timestamp"))) \
        .withColumn("month", month(col("Timestamp"))) \
        .withColumn("Timestamp", unix_timestamp("Timestamp", "yyyy-MM-dd HH:mm:ss"))
    return preprocessed_data

def create_pipeline(use_standard_scaler=False, use_gbt=False):
    """
    Create the machine learning pipeline with feature engineering and model training stages
    """
    event_indexer = StringIndexer(inputCol="EventType", outputCol="EventTypeIndex")
    location_indexer = StringIndexer(inputCol="Location", outputCol="LocationIndex")
    severity_indexer = StringIndexer(inputCol="Severity", outputCol="SeverityIndex")

    event_encoder = OneHotEncoder(inputCol="EventTypeIndex", outputCol="EventTypeVec")
    location_encoder = OneHotEncoder(inputCol="LocationIndex", outputCol="LocationVec")
    severity_encoder = OneHotEncoder(inputCol="SeverityIndex", outputCol="SeverityVec")

    assembler = VectorAssembler(
        inputCols=["Timestamp", "days_since_event", "hour", "day_of_week", "month", "EventTypeVec", "LocationVec", "SeverityVec"],
        outputCol="features"
    )

    if use_standard_scaler:
        scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True)
    else:
        scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

    if use_gbt:
        classifier = GBTClassifier(labelCol="Is_Anomaly", featuresCol="scaledFeatures")
    else:
        classifier = RandomForestClassifier(labelCol="Is_Anomaly", featuresCol="scaledFeatures", numTrees=100)

    pipeline = Pipeline(stages=[
        event_indexer, location_indexer, severity_indexer,
        event_encoder, location_encoder, severity_encoder,
        assembler, scaler, classifier
    ])

    return pipeline

def train_pipeline(pipeline, train_data):
    """
    Train the machine learning pipeline using cross-validation and hyperparameter tuning
    """
    classifier = pipeline.getStages()[-1]
    
    if isinstance(classifier, GBTClassifier):
        impurity_values = ["variance"]
    else:
        impurity_values = ["gini", "entropy"]
    
    param_grid = ParamGridBuilder() \
        .addGrid(classifier.maxDepth, [5, 10, 15]) \
        .addGrid(classifier.maxBins, [16, 32, 64]) \
        .addGrid(classifier.impurity, impurity_values) \
        .build()

    binary_evaluator = BinaryClassificationEvaluator(labelCol="Is_Anomaly")

    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=param_grid, evaluator=binary_evaluator, numFolds=3, parallelism=4)
    model = cv.fit(train_data)

    return model

def evaluate_model(model, test_data):
    """
    Evaluate the trained model on the test dataset
    """
    predictions = model.transform(test_data)

    binary_evaluator = BinaryClassificationEvaluator(labelCol="Is_Anomaly")
    multiclass_evaluator = MulticlassClassificationEvaluator(labelCol="Is_Anomaly", metricName="accuracy")

    binary_metrics = binary_evaluator.evaluate(predictions)
    multiclass_accuracy = multiclass_evaluator.evaluate(predictions)

    print(f"Binary Classification Metrics: {binary_metrics:.4f}")
    print(f"Multiclass Classification Accuracy: {multiclass_accuracy:.4f}")

    return binary_metrics, multiclass_accuracy

def retrain_model(model, new_data):
    """
    Retrain the machine learning model with new data
    """
    preprocessed_data = preprocess_data(new_data)
    retrained_model = model.fit(preprocessed_data)
    return retrained_model

def make_predictions(model, data):
    """
    Make predictions using the trained model on the input data
    """
    preprocessed_data = preprocess_data(data)
    predictions = model.transform(preprocessed_data)
    return predictions

def train_model(data, use_standard_scaler=False, use_gbt=False):
    """
    Train the machine learning model using the input data
    """
    preprocessed_data = preprocess_data(data)
    train_data, test_data = preprocessed_data.randomSplit([0.8, 0.2], seed=42)

    pipeline = create_pipeline(use_standard_scaler, use_gbt)
    model = train_pipeline(pipeline, train_data)
    binary_metrics, multiclass_accuracy = evaluate_model(model, test_data)

    return model, binary_metrics, multiclass_accuracy

def deploy_model(model, version):
    """
    Deploy the trained model with versioning and symlink creation for zero-downtime deployment
    """
    model_path = os.path.join(MODEL_DIR, f"health_risk_model_v{version}")
    model.write().overwrite().save(model_path)

    latest_model_path = os.path.join(MODEL_DIR, LATEST_MODEL_DIR)
    temp_symlink = latest_model_path + "_temp"
    os.symlink(model_path, temp_symlink)
    os.rename(temp_symlink, latest_model_path)

def load_latest_model():
    """
    Load the latest deployed model
    """
    latest_model_path = os.path.join(MODEL_DIR, LATEST_MODEL_DIR)
    if os.path.exists(latest_model_path):
        return PipelineModel.load(latest_model_path)
    else:
        return None
    
def make_predictions_and_deploy(batch_df):
    """
    Make predictions on the batch data using the latest deployed model and show the predictions
    """
    model = load_latest_model()

    if model is not None:
        predictions = make_predictions(model, batch_df)
        predictions = predictions.select("EventType", "Timestamp", "Location", "Severity", "Details", "prediction")
        predictions = predictions.withColumn("Is_Anomaly", col("prediction").cast("string"))
        predictions = predictions.drop("prediction")
        
        # Save the predicted outbreaks data as a Parquet file
        predictions.write.mode("append").parquet("models/predicted_outbreaks")
        
        predictions.show()

def main(spark):
    # load the initial dataset
    data = spark.read.csv("data/1m_health_events_dataset.csv", header=True, inferSchema=True)

    # train the initial model using standard scaler and gradient boosted trees
    model, initial_binary_metrics, initial_multiclass_accuracy = train_model(data, use_standard_scaler=True, use_gbt=True)
    deploy_model(model, version=1)

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

    # define the window duration and slide interval
    window_duration = "1 minute"
    slide_interval = "30 seconds"

    # apply windowing to the streaming data
    windowed_df = parsed_df \
        .withWatermark("Timestamp", "1 minute") \
        .groupBy(window("Timestamp", window_duration, slide_interval), "EventType", "Location", "Severity") \
        .count()

    # start the prediction service
    prediction_service = windowed_df \
        .writeStream \
        .foreachBatch(lambda batch_df, _: make_predictions_and_deploy(batch_df)) \
        .start()

    model_version = 1
    while True:
        # consume new data from Kafka for retraining
        new_data = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        # load the latest deployed model
        latest_model = load_latest_model()

        # retrain the model with the new data
        retrained_model, retrained_binary_metrics, retrained_multiclass_accuracy = retrain_model(latest_model, new_data)

        # compare the performance of the retrained model with the current model
        if retrained_binary_metrics > initial_binary_metrics and retrained_multiclass_accuracy > initial_multiclass_accuracy:
            model_version += 1
            deploy_model(retrained_model, version=model_version)
            initial_binary_metrics = retrained_binary_metrics
            initial_multiclass_accuracy = retrained_multiclass_accuracy
            print(f"Deployed new model version: {model_version}")
        else:
            print("Retrained model performance is not better. Keeping the current model.")

        # wait for the specified retrain interval
        sleep(RETRAIN_INTERVAL)

    prediction_service.awaitTermination()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("HealthRiskPrediction").getOrCreate()
    main(spark)