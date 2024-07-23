"""
This module contains the functions to process the analytical data layer.
"""

import os
import glob

from pyspark.sql.functions import col, avg, sum

from src.utils.spark_session import SparkSessionManager
from src.utils.singleton_logger import SingletonLogger

# Get the Spark session and logger
spark = SparkSessionManager.get_instance()
logger = SingletonLogger().get_logger()


def read_curated_data(region, year, month):
    """
    Read curated data for a given region, year, and month.
    :param region: Region to read data for
    :param year: Year to read data for
    :param month: Month to read data for
    :return: DataFrame containing the curated data
    :rtype: DataFrame
    """
    input_path = f"data/curated/{region}/{year}/curated_data_{month}.parquet"
    df = spark.read.parquet(input_path)
    return df


def transform_and_aggregate_data(df):
    """
    Transform and aggregate the data.
    :param df: DataFrame containing the curated data
    :return: Aggregated DataFrame
    :rtype: DataFrame
    """
    # Example transformations:
    # - Calculate monthly average demand and price
    df_aggregated = df.groupBy("date").agg(
        avg(col("avg_demand")).alias("monthly_avg_demand"),
        avg(col("avg_rrp")).alias("monthly_avg_rrp"),
        sum(col("total_demand")).alias("monthly_total_demand"),
        sum(col("total_rrp")).alias("monthly_total_rrp")
    )
    return df_aggregated


def write_analytical_data(df_aggregated, region, year, month):
    """
    Write the aggregated data to the analytical data layer.
    :param df_aggregated: Aggregated DataFrame
    :param region: Region to write data for
    :param year: Year to write data for
    :param month: Month to write data for
    :return: None
    :rtype: None
    """
    output_path = f"data/analytical/{region}/{year}/analytical_data_{month}.parquet"
    df_aggregated.write.mode("overwrite").parquet(output_path)


def process_analytical_data(region, year):
    """
    Process curated data for a given region and year, aggregate it, and write the aggregated data to the analytical
    layer.
    :param region: Region to process
    :param year: Year to process
    :return: None
    :rtype: None
    """
    input_path = f"data/curated/{region}/{year}/*.parquet"
    output_path = f"data/analytical/{region}/{year}/"

    # Get list of all Parquet files for the region and year
    parquet_files = glob.glob(input_path)

    for parquet_file in parquet_files:
        # Read and clean the Parquet file
        file_name = os.path.basename(parquet_file)
        month = file_name.split("_")[2].split(".")[0]  # Extract "202401" from "curated_data_202401.parquet"

        df = read_curated_data(region, year, month)
        df_aggregated = transform_and_aggregate_data(df)
        write_analytical_data(df_aggregated, region, year, month)
