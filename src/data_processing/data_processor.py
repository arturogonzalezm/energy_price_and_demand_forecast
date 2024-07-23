"""
This module contains the DataProcessor abstract class and its concrete implementations for the staging, curated, and
analytical layers. Each implementation defines the methods to read, clean, transform, and write data for that layer.
"""

import os
import glob
from abc import ABC, abstractmethod

from pyspark.sql.functions import col, avg, sum as sql_sum, to_timestamp, lag
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


class DataProcessor(ABC):
    """
    Abstract base class for data processors.
    """

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def process_data(self, region, year):
        """Template method defining the skeleton of the algorithm."""
        try:
            input_files = self.get_input_files(region, year)
            if not input_files:
                self.logger.warning("No input files found for %s in %s", region, year)
                return

            self.logger.info("Found %d files to process for %s in %s", len(input_files), region, year)
            for input_file in input_files:
                try:
                    self.logger.info("Processing file: %s", input_file)
                    df = self.read_data(input_file)
                    self.logger.info("Read %d rows from %s", df.count(), input_file)
                    df_cleaned = self.clean_data(df)
                    self.logger.info("Cleaned data, now have %d rows", df_cleaned.count())
                    df_transformed = self.transform_data(df_cleaned)
                    self.logger.info("Transformed data, now have %d rows", df_transformed.count())
                    df_features = self.feature_engineering(df_transformed)
                    self.logger.info("Feature engineered data, now have %d rows", df_features.count())
                    month = self.extract_month(input_file)
                    self.write_data(df_features, region, year, month)
                    self.logger.info("Completed processing %s", input_file)
                except Exception as e:
                    self.logger.error("Error processing file %s: %s", input_file, str(e))
        except Exception as e:
            self.logger.error("Error in process_data for %s in %s: %s", region, year, str(e))

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
    def feature_engineering(self, df):
        """Perform feature engineering on the transformed data."""
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
    """
    Data processor for the staging layer.
    """

    def get_input_files(self, region, year):
        """Get list of input files to process."""
        return glob.glob(f"data/raw/{region}/{year}/*.csv")

    def read_data(self, input_file):
        """Read data from the input file."""
        schema = StructType([
            StructField("REGION", StringType(), True),
            StructField("SETTLEMENTDATE", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
            StructField("RRP", DoubleType(), True),
            StructField("PERIODTYPE", StringType(), True),
        ])
        return self.spark.read.csv(input_file, header=True, schema=schema)

    def clean_data(self, df):
        """Clean the input data."""
        return df.withColumn("date", to_timestamp(col("SETTLEMENTDATE"), "yyyy/MM/dd HH:mm:ss")) \
            .drop("SETTLEMENTDATE") \
            .na.fill(0)

    def transform_data(self, df):
        """Transform the cleaned data."""
        return df  # No additional transformation for staging data

    def feature_engineering(self, df):
        """Perform feature engineering on the transformed data."""
        window_spec = Window.partitionBy("REGION").orderBy("date")
        df = df.withColumn("prev_total_demand", lag("TOTALDEMAND").over(window_spec)) \
               .withColumn("prev_rrp", lag("RRP").over(window_spec))
        df = df.withColumn("demand_diff", col("TOTALDEMAND") - col("prev_total_demand")) \
               .withColumn("rrp_diff", col("RRP") - col("prev_rrp"))
        return df

    def write_data(self, df, region, year, month):
        """Write the processed data."""
        output_path = f"data/staging/{region}/{year}/cleaned_data_{month}.parquet"
        df.write.mode("overwrite").parquet(output_path)
        self.logger.info("Wrote staging data to: %s", output_path)

    def extract_month(self, file_name):
        """Extract month from the file name."""
        return os.path.basename(file_name).split("_")[3][:6]


class CuratedDataProcessor(DataProcessor):
    """
    Data processor for the curated layer.
    """

    def get_input_files(self, region, year):
        """Get list of input files to process."""
        return glob.glob(f"data/staging/{region}/{year}/*.parquet")

    def read_data(self, input_file):
        """Read data from the input file."""
        return self.spark.read.parquet(input_file)

    def clean_data(self, df):
        """Clean the input data."""
        if 'SETTLEMENTDATE' in df.columns:
            return df.withColumn("date", to_timestamp(col("SETTLEMENTDATE"), "yyyy-MM-dd HH:mm:ss")) \
                .drop("SETTLEMENTDATE") \
                .na.fill(0)
        return df.na.fill(0)

    def transform_data(self, df):
        """Transform the cleaned data."""
        return df.groupBy("date").agg(
            avg(col("TOTALDEMAND")).alias("avg_demand"),
            avg(col("RRP")).alias("avg_rrp"),
            sql_sum(col("TOTALDEMAND")).alias("total_demand"),
            sql_sum(col("RRP")).alias("total_rrp")
        )

    def feature_engineering(self, df):
        """Perform feature engineering on the transformed data."""
        df = df.withColumn("demand_rrp_ratio", col("total_demand") / col("total_rrp"))
        return df

    def write_data(self, df, region, year, month):
        """Write the processed data."""
        output_path = f"data/curated/{region}/{year}/curated_data_{month}.parquet"
        df.write.mode("overwrite").parquet(output_path)
        self.logger.info("Wrote curated data to: %s", output_path)

    def extract_month(self, file_name):
        """Extract month from the file name."""
        return os.path.basename(file_name).split("_")[2].split(".")[0]


class AnalyticalDataProcessor(DataProcessor):
    """
    Data processor for the analytical layer.
    """

    def get_input_files(self, region, year):
        """Get list of input files to process."""
        return glob.glob(f"data/curated/{region}/{year}/*.parquet")

    def read_data(self, input_file):
        """Read data from the input file."""
        return self.spark.read.parquet(input_file)

    def clean_data(self, df):
        """Clean the input data."""
        return df  # Data is already cleaned in the curated layer

    def transform_data(self, df):
        """Transform the cleaned data."""
        return df.groupBy("date").agg(
            avg(col("avg_demand")).alias("monthly_avg_demand"),
            avg(col("avg_rrp")).alias("monthly_avg_rrp"),
            sql_sum(col("total_demand")).alias("monthly_total_demand"),
            sql_sum(col("total_rrp")).alias("monthly_total_rrp")
        )

    def feature_engineering(self, df):
        """Perform feature engineering on the transformed data."""
        df = df.withColumn("demand_rrp_ratio", col("monthly_total_demand") / col("monthly_total_rrp"))
        return df

    def write_data(self, df, region, year, month):
        """Write the processed data."""
        output_path = f"data/analytical/{region}/{year}/analytical_data_{month}.parquet"
        df.write.mode("overwrite").parquet(output_path)
        self.logger.info("Wrote analytical data to: %s", output_path)

    def extract_month(self, file_name):
        """Extract month from the file name."""
        return os.path.basename(file_name).split("_")[2].split(".")[0]
