"""
This module contains the data processor for the staging layer.
"""
import os
import glob

from pyspark.sql.functions import col, avg, sum as sql_sum, to_timestamp, lag
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.data_processing.data_processor import DataProcessor


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
