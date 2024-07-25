"""
This module contains the data processor for the curated layer.
"""
import os
import glob

from pyspark.sql.functions import col, avg, sum as sql_sum, to_timestamp

from src.data_processing.data_processor import DataProcessor


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
