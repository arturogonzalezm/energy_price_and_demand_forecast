"""
T
"""

import os
import glob

from pyspark.sql.functions import col, avg, sum, quarter, countDistinct, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql import functions as F
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession

from src.utils.spark_session import SparkSessionManager
from src.utils.singleton_logger import SingletonLogger

spark = SparkSessionManager.get_instance()
logger = SingletonLogger().get_logger()

# Define the schema for your data
schema = StructType([
    StructField("REGION", StringType(), True),
    StructField("SETTLEMENTDATE", StringType(), True),
    StructField("TOTALDEMAND", DoubleType(), True),
    StructField("RRP", DoubleType(), True),
    StructField("PERIODTYPE", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("monthly_avg_demand", DoubleType(), True),
    StructField("monthly_avg_rrp", DoubleType(), True),
    StructField("monthly_total_demand", DoubleType(), True),
    StructField("monthly_total_rrp", DoubleType(), True)
])

MIN_REQUIRED_SAMPLES = 5  # Minimum samples required for training


def read_analytical_data(region, year, month):
    input_path = f"data/analytical/{region}/{year}/analytical_data_{month}.parquet"
    df = spark.read.schema(schema).parquet(input_path)
    # Ensure REGION column is present
    df = df.withColumn("REGION", lit(region))
    return df


def aggregate_data(df):
    df_aggregated = df.withColumn("quarter", quarter(col("date"))).groupBy("REGION", "quarter").agg(
        avg("monthly_avg_demand").alias("avg_demand"),
        avg("monthly_avg_rrp").alias("avg_rrp"),
        sum("monthly_total_demand").alias("total_demand"),
        sum("monthly_total_rrp").alias("total_rrp")
    )
    return df_aggregated


def prepare_ml_data(df):
    df = aggregate_data(df)
    df = df.withColumn("demand_rrp_ratio", col("total_demand") / col("total_rrp"))

    feature_columns = ["avg_demand", "avg_rrp", "total_demand", "total_rrp", "demand_rrp_ratio"]

    # Check for null values
    for column in feature_columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            logger.warning(f"Column {column} contains {null_count} null values")

    # Remove rows with null values
    df = df.dropna(subset=feature_columns)

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df_ml = assembler.transform(df)

    df_ml = df_ml.withColumnRenamed("avg_rrp", "label")

    # UDF to check the number of non-zero elements in the vector
    def num_nonzeros(v):
        return int(v.numNonzeros())

    num_nonzeros_udf = F.udf(num_nonzeros, IntegerType())

    # Validate that features column is not empty
    empty_features = df_ml.filter(num_nonzeros_udf(df_ml.features) == 0).count()
    if empty_features > 0:
        logger.warning(f"Found {empty_features} rows with empty feature vectors")

    # Filter out rows with empty feature vectors
    df_ml = df_ml.filter(num_nonzeros_udf(df_ml.features) > 0)

    return df_ml


def check_variance(df_ml):
    label_summary = df_ml.agg(countDistinct("label")).collect()
    distinct_label_count = label_summary[0][0]

    if distinct_label_count <= 1:
        logger.info("Label column has insufficient variance. Skipping model training.")
        return False

    return True


def train_model(df_ml):
    if df_ml.count() < MIN_REQUIRED_SAMPLES:
        logger.warning(
            f"Insufficient data for modeling. Found {df_ml.count()} samples, need at least {MIN_REQUIRED_SAMPLES}")
        return None, df_ml.withColumn("prediction", col("label"))

    train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)

    logger.info(f"Training data count: {train_data.count()}")
    logger.info(f"Test data count: {test_data.count()}")

    if train_data.count() == 0:
        raise ValueError("Training data is empty. Cannot train the model.")

    if test_data.count() == 0:
        logger.info("Test data is empty. Using training data for evaluation.")
        test_data = train_data

    if not check_variance(train_data):
        predictions = train_data.withColumn("prediction", col("label"))
        return None, predictions

    lr = LinearRegression(featuresCol="features", labelCol="label")

    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1, 0.5, 1.0]) \
        .build()

    num_folds = min(5, max(2, df_ml.count() // 10))  # Ensure at least 2 folds, but no more than 5
    crossval = CrossValidator(estimator=lr,
                              estimatorParamMaps=paramGrid,
                              evaluator=RegressionEvaluator(predictionCol="prediction", labelCol="label",
                                                            metricName="rmse"),
                              numFolds=num_folds)

    cvModel = crossval.fit(train_data)

    predictions = cvModel.transform(test_data)

    logger.info(f"Predictions data count: {predictions.count()}")
    if predictions.count() == 0:
        raise ValueError("Predictions data is empty. Cannot evaluate the model.")

    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    logger.info(f"Root Mean Squared Error (RMSE): {rmse}")

    return cvModel, predictions


def save_predictions(predictions, region, year, month, output_path):
    predictions.write.mode("overwrite").parquet(output_path)


def process_ml_forecasting_data(region, year):
    input_path = f"data/analytical/{region}/{year}/*.parquet"
    output_path = f"data/ml_forecasting/{region}/{year}/"

    parquet_files = glob.glob(input_path)

    for parquet_file in parquet_files:
        file_name = os.path.basename(parquet_file)
        month = file_name.split("_")[2].split(".")[0]

        df = read_analytical_data(region, year, month)

        logger.info(f"Analytical data count for {region} {year} {month}: {df.count()}")
        logger.info(f"Analytical data schema: {df.schema}")

        if df.count() == 0:
            logger.info(f"Skipping empty analytical data for {region} {year} {month}.")
            continue

        df_ml = prepare_ml_data(df)

        logger.info(f"ML data count for {region} {year} {month}: {df_ml.count()}")
        logger.info(f"ML data schema: {df_ml.schema}")

        if df_ml.count() == 0:
            logger.info(f"Skipping empty ML data for {region} {year} {month}.")
            continue

        model, predictions = train_model(df_ml)
        if model is not None:
            prediction_output_path = os.path.join(output_path, f"predictions_{month}.parquet")
            save_predictions(predictions, region, year, month, prediction_output_path)
        else:
            logger.info(f"No model trained for {region} {year} {month}. Skipping prediction saving.")
