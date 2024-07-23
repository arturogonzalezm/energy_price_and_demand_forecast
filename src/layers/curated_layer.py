import os
import glob

from pyspark.sql.functions import col, avg, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.utils.spark_session import SparkSessionManager
from src.utils.singleton_logger import SingletonLogger

# Initialize Spark session
spark = SparkSessionManager.get_instance("CuratedDataLayer")
logger = SingletonLogger().get_logger()

# Define the schema for your data
schema = StructType([
    StructField("REGION", StringType(), True),
    StructField("SETTLEMENTDATE", StringType(), True),
    StructField("TOTALDEMAND", DoubleType(), True),
    StructField("RRP", DoubleType(), True),
    StructField("PERIODTYPE", StringType(), True),
])


def read_staged_data(region, year, month):
    input_path = f"data/staging/{region}/{year}/cleaned_data_{month}.parquet"
    df = spark.read.parquet(input_path)
    return df


def transform_and_curate_data(df):
    # Check if SETTLEMENTDATE exists and convert it to date if necessary
    if 'SETTLEMENTDATE' in df.columns:
        df = df.withColumn("date", to_timestamp(col("SETTLEMENTDATE"), "yyyy-MM-dd HH:mm:ss")) \
            .drop("SETTLEMENTDATE")

    # Perform data cleaning and transformation
    df = df.na.fill(0)  # Replace null values with 0

    # Example transformations:
    # - Calculate average demand and price per day
    df_curated = df.groupBy("date").agg(
        avg(col("TOTALDEMAND")).alias("avg_demand"),
        avg(col("RRP")).alias("avg_rrp"),
        sum(col("TOTALDEMAND")).alias("total_demand"),
        sum(col("RRP")).alias("total_rrp")
    )
    return df_curated


def write_curated_data(df_curated, region, year, month):
    output_path = f"data/curated/{region}/{year}/curated_data_{month}.parquet"
    df_curated.write.mode("overwrite").parquet(output_path)


def process_curated_data(region, year):
    input_path = f"data/staging/{region}/{year}/*.parquet"
    output_path = f"data/curated/{region}/{year}/"

    # Get list of all Parquet files for the region and year
    parquet_files = glob.glob(input_path)

    for parquet_file in parquet_files:
        # Read and clean the Parquet file
        file_name = os.path.basename(parquet_file)
        month = file_name.split("_")[2].split(".")[0]  # Extract "202401" from "cleaned_data_202401.parquet"

        df = read_staged_data(region, year, month)
        df_curated = transform_and_curate_data(df)
        write_curated_data(df_curated, region, year, month)
