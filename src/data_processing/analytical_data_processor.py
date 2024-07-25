"""
Data processor for the analytical layer.
"""

import os
import glob

from pyspark.sql.functions import col, avg, sum as sql_sum

from src.data_processing.data_processor import DataProcessor


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
