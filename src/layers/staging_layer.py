import os
import glob
from pyspark.sql.functions import col, to_timestamp

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.utils.spark_session import SparkSessionManager
from src.utils.singleton_logger import SingletonLogger

spark = SparkSessionManager.get_instance("StagingDataLayer")
logger = SingletonLogger().get_logger()

# Define the schema for your data
schema = StructType([
    StructField("REGION", StringType(), True),
    StructField("SETTLEMENTDATE", StringType(), True),
    StructField("TOTALDEMAND", DoubleType(), True),
    StructField("RRP", DoubleType(), True),
    StructField("PERIODTYPE", StringType(), True),
])


def read_and_clean_csv(input_path):
    # Read CSV file into DataFrame with predefined schema
    df = spark.read.csv(input_path, header=True, schema=schema)

    # Perform data cleaning and transformation
    df_cleaned = df.withColumn("date", to_timestamp(col("SETTLEMENTDATE"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("SETTLEMENTDATE") \
        .na.fill(0)  # Replace null values with 0

    return df_cleaned


def process_region_data(region, year):
    input_path = f"data/raw/{region}/{year}/*.csv"
    output_path = f"data/staging/{region}/{year}/"

    # Get list of all CSV files for the region and year
    csv_files = glob.glob(input_path)

    for csv_file in csv_files:
        # Read and clean the CSV file
        df_cleaned = read_and_clean_csv(csv_file)

        # Extract the month from the file name (assuming the file name format is consistent)
        file_name = os.path.basename(csv_file)
        month = file_name.split("_")[3][:6]  # Extract "202401" from "PRICE_AND_DEMAND_202401_NSW1.csv"

        # Write the cleaned DataFrame to Parquet file
        df_cleaned.write.mode("overwrite").parquet(os.path.join(output_path, f"cleaned_data_{month}.parquet"))
