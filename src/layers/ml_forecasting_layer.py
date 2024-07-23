import os
import glob

from pyspark.sql.functions import col, avg, sum, quarter, countDistinct, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from src.utils.spark_session import SparkSessionManager
from src.utils.singleton_logger import SingletonLogger

spark = SparkSessionManager.get_instance("MLForecastingLayer")
logger = SingletonLogger().get_logger()


def read_analytical_data(region, year, month):
    input_path = f"data/analytical/{region}/{year}/analytical_data_{month}.parquet"
    df = spark.read.parquet(input_path)
    # Ensure REGION column is present
    df = df.withColumn("REGION", lit(region))
    return df


def aggregate_data(df):
    # Aggregate data over a longer period, for example, quarterly
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
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df_ml = assembler.transform(df)

    df_ml = df_ml.withColumnRenamed("avg_rrp", "label")
    return df_ml


def check_variance(df_ml):
    label_summary = df_ml.agg(countDistinct("label")).collect()
    distinct_label_count = label_summary[0][0]

    if distinct_label_count <= 1:
        logger.info("Label column has insufficient variance. Skipping model training.")
        return False

    return True


def train_model(df_ml):
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

    crossval = CrossValidator(estimator=lr,
                              estimatorParamMaps=paramGrid,
                              evaluator=RegressionEvaluator(predictionCol="prediction", labelCol="label",
                                                            metricName="rmse"),
                              numFolds=5)

    cvModel = crossval.fit(train_data)

    predictions = cvModel.transform(test_data)

    logger.info(f"Predictions data count: {predictions.count()}")
    if predictions.count() == 0:
        raise ValueError("Predictions data is empty. Cannot evaluate the model.")

    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    logger.info(f"Root Mean Squared Error (RMSE): {rmse}")

    return cvModel, predictions


def save_predictions(predictions, region, year, month):
    output_path = f"data/ml_forecasting/{region}/{year}/predictions_{month}.parquet"
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
        if df.count() == 0:
            logger.info(f"Skipping empty analytical data for {region} {year} {month}.")
            continue

        df_ml = prepare_ml_data(df)

        logger.info(f"ML data count for {region} {year} {month}: {df_ml.count()}")
        if df_ml.count() == 0:
            logger.info(f"Skipping empty ML data for {region} {year} {month}.")
            continue

        model, predictions = train_model(df_ml)
        save_predictions(predictions, region, year, month)
