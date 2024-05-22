""" Local Model Trainer for Health Risk Prediction System """
# local_model_trainer.py
# pyright: reportAttributeAccessIssue=false

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, datediff, current_timestamp, hour, dayofweek, month
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, MinMaxScaler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# constants
MODEL_DIR = "models"
LOCAL_MODEL_DIR = "local_health_risk_model"

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

def save_model(model):
    """
    Save the trained model locally
    """
    model_path = os.path.join(MODEL_DIR, LOCAL_MODEL_DIR)
    model.write().overwrite().save(model_path)
    print(f"Model saved at: {model_path}")

def main(spark):
    # load the local dataset
    data = spark.read.csv("data/1m_health_events_dataset.csv", header=True, inferSchema=True)

    # train the model using standard scaler and gradient boosted trees
    model, binary_metrics, multiclass_accuracy = train_model(data, use_standard_scaler=True, use_gbt=True)
    
    # save the trained model locally
    save_model(model)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LocalHealthRiskPrediction").getOrCreate()
    main(spark)