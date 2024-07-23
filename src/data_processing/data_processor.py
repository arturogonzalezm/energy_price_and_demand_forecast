"""
This module contains the DataProcessor abstract class and its concrete implementations for the staging, curated, and
analytical layers. Each implementation defines the methods to read, clean, transform, and write data for that layer.
"""

import os
import glob
from abc import ABC, abstractmethod

from pyspark.sql.functions import col, avg, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


class DataProcessor(ABC):
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def process_data(self, region, year):
        """Template method defining the skeleton of the algorithm."""
        try:
            input_files = self.get_input_files(region, year)
            if not input_files:
                self.logger.warning(f"No input files found for {region} in {year}")
                return

            self.logger.info(f"Found {len(input_files)} files to process for {region} in {year}")
            for input_file in input_files:
                try:
                    self.logger.info(f"Processing file: {input_file}")
                    df = self.read_data(input_file)
                    self.logger.info(f"Read {df.count()} rows from {input_file}")
                    df_cleaned = self.clean_data(df)
                    self.logger.info(f"Cleaned data, now have {df_cleaned.count()} rows")
                    df_transformed = self.transform_data(df_cleaned)
                    self.logger.info(f"Transformed data, now have {df_transformed.count()} rows")
                    month = self.extract_month(input_file)
                    self.write_data(df_transformed, region, year, month)
                    self.logger.info(f"Completed processing {input_file}")
                except Exception as e:
                    self.logger.error(f"Error processing file {input_file}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error in process_data for {region} in {year}: {str(e)}")

    @abstractmethod
    def get_input_files(self, region, year):
        """Get list of input files to process."""
        pass

    @abstractmethod
    def read_data(self, input_file):
        """Read data from the input file."""
        pass

    @abstractmethod
    def clean_data(self, df):
        """Clean the input data."""
        pass

    @abstractmethod
    def transform_data(self, df):
        """Transform the cleaned data."""
        pass

    @abstractmethod
    def write_data(self, df, region, year, month):
        """Write the processed data."""
        pass

    @abstractmethod
    def extract_month(self, file_name):
        """Extract month from the file name."""
        pass


class StagingDataProcessor(DataProcessor):
    def get_input_files(self, region, year):
        """
        Get list of input files to process.
        :param region:
        :param year:
        :return:
        """
        return glob.glob(f"data/raw/{region}/{year}/*.csv")

    def read_data(self, input_file):
        """
        Read data from the input file.
        :param input_file:
        :return:
        """
        schema = StructType([
            StructField("REGION", StringType(), True),
            StructField("SETTLEMENTDATE", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
            StructField("RRP", DoubleType(), True),
            StructField("PERIODTYPE", StringType(), True),
        ])
        return self.spark.read.csv(input_file, header=True, schema=schema)

    def clean_data(self, df):
        """
        Clean the input data.
        :param df:
        :return:
        """
        return df.withColumn("date", to_timestamp(col("SETTLEMENTDATE"), "yyyy/MM/dd HH:mm:ss")) \
            .drop("SETTLEMENTDATE") \
            .na.fill(0)

    def transform_data(self, df):
        """
        Transform the cleaned data.
        :param df:
        :return:
        """
        return df  # No additional transformation for staging data

    def write_data(self, df, region, year, month):
        """
        Write the processed data.
        :param df:
        :param region:
        :param year:
        :param month:
        :return:
        """
        output_path = f"data/staging/{region}/{year}/cleaned_data_{month}.parquet"
        df.write.mode("overwrite").parquet(output_path)
        self.logger.info(f"Wrote staging data to: {output_path}")

    def extract_month(self, file_name):
        """
        Extract month from the file name.
        :param file_name:
        :return:
        """
        return os.path.basename(file_name).split("_")[3][:6]


class CuratedDataProcessor(DataProcessor):
    def get_input_files(self, region, year):
        """
        Get list of input files to process.
        :param region:
        :param year:
        :return:
        """
        return glob.glob(f"data/staging/{region}/{year}/*.parquet")

    def read_data(self, input_file):
        """
        Read data from the input file.
        :param input_file:
        :return:
        """
        return self.spark.read.parquet(input_file)

    def clean_data(self, df):
        """
        Clean the input data.
        :param df:
        :return:
        """
        if 'SETTLEMENTDATE' in df.columns:
            return df.withColumn("date", to_timestamp(col("SETTLEMENTDATE"), "yyyy-MM-dd HH:mm:ss")) \
                .drop("SETTLEMENTDATE") \
                .na.fill(0)
        return df.na.fill(0)

    def transform_data(self, df):
        """
        Transform the cleaned data.
        :param df:
        :return:
        """
        return df.groupBy("date").agg(
            avg(col("TOTALDEMAND")).alias("avg_demand"),
            avg(col("RRP")).alias("avg_rrp"),
            sum(col("TOTALDEMAND")).alias("total_demand"),
            sum(col("RRP")).alias("total_rrp")
        )

    def write_data(self, df, region, year, month):
        """
        Write the processed data.
        :param df:
        :param region:
        :param year:
        :param month:
        :return:
        """
        output_path = f"data/curated/{region}/{year}/curated_data_{month}.parquet"
        df.write.mode("overwrite").parquet(output_path)
        self.logger.info(f"Wrote curated data to: {output_path}")

    def extract_month(self, file_name):
        """
        Extract month from the file name.
        :param file_name:
        :return:
        """
        return os.path.basename(file_name).split("_")[2].split(".")[0]


class AnalyticalDataProcessor(DataProcessor):
    def get_input_files(self, region, year):
        """
        Get list of input files to process.
        :param region:
        :param year:
        :return:
        """
        return glob.glob(f"data/curated/{region}/{year}/*.parquet")

    def read_data(self, input_file):
        """
        Read data from the input file.
        :param input_file:
        :return:
        """
        return self.spark.read.parquet(input_file)

    def clean_data(self, df):
        """
        Clean the input data.
        :param df:
        :return:
        """
        return df  # Data is already cleaned in the curated layer

    def transform_data(self, df):
        """
        Transform the cleaned data.
        :param df:
        :return:
        """
        return df.groupBy("date").agg(
            avg(col("avg_demand")).alias("monthly_avg_demand"),
            avg(col("avg_rrp")).alias("monthly_avg_rrp"),
            sum(col("total_demand")).alias("monthly_total_demand"),
            sum(col("total_rrp")).alias("monthly_total_rrp")
        )

    def write_data(self, df, region, year, month):
        """
        Write the processed data.
        :param df:
        :param region:
        :param year:
        :param month:
        :return:
        """
        output_path = f"data/analytical/{region}/{year}/analytical_data_{month}.parquet"
        df.write.mode("overwrite").parquet(output_path)
        self.logger.info(f"Wrote analytical data to: {output_path}")

    def extract_month(self, file_name):
        """
        Extract month from the file name.
        :param file_name:
        :return:
        """
        return os.path.basename(file_name).split("_")[2].split(".")[0]
