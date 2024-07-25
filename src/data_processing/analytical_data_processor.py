"""
Data processor for the analytical layer.
"""
import glob
import os
from pyspark.sql.functions import col, avg, sum as sql_sum, stddev, abs as abs_func
from pyspark.sql.window import Window
from src.data_processing.data_processor import DataProcessor


class AnalyticalDataProcessor(DataProcessor):
    """
    Data processor for the analytical layer.
    """

    def get_input_files(self, region, year):
        """
        Get list of input files to process.
        :param region: The region for which to get input files.
        :param year: The year for which to get input files.
        :return: A list of input file paths.
        """
        return glob.glob(f"data/curated/{region}/{year}/*.parquet")

    def read_data(self, input_file):
        """
        Read data from the input file.
        :param input_file: The path to the input file.
        :return: A DataFrame object.
        """
        return self.spark.read.parquet(input_file)

    def clean_data(self, df):
        """
        Clean the input data.
        :param df: The input DataFrame.
        :return: The cleaned DataFrame.
        """
        return df  # Data is already cleaned in the curated layer

    def transform_data(self, df):
        """
        Transform the cleaned data.
        :param df: The cleaned DataFrame.
        :return: The transformed DataFrame.
        """
        return df.groupBy("date").agg(
            avg(col("avg_demand")).alias("monthly_avg_demand"),
            avg(col("avg_rrp")).alias("monthly_avg_rrp"),
            sql_sum(col("total_demand")).alias("monthly_total_demand"),
            sql_sum(col("total_rrp")).alias("monthly_total_rrp")
        )

    def feature_engineering(self, df):
        """
        Perform feature engineering on the transformed data and detect anomalies.
        :param df: The transformed DataFrame.
        :return: The DataFrame with additional features and anomaly detection.
        """
        df = df.withColumn("demand_rrp_ratio", col("monthly_total_demand") / col("monthly_total_rrp"))

        # Anomaly detection using Z-score
        window_spec = Window.orderBy("date")
        df = df.withColumn("mean_ratio", avg("demand_rrp_ratio").over(window_spec))
        df = df.withColumn("stddev_ratio", stddev("demand_rrp_ratio").over(window_spec))
        df = df.withColumn("z_score", (col("demand_rrp_ratio") - col("mean_ratio")) / col("stddev_ratio"))

        # Identify anomalies where the absolute Z-score is greater than a threshold (e.g., 3)
        anomaly_threshold = 3
        df = df.withColumn("is_anomaly", abs_func(col("z_score")) > anomaly_threshold)

        return df

    def write_data(self, df, region, year, month):
        """
        Write the processed data.
        :param df: The DataFrame to write.
        :param region: The region for which the data is being processed.
        :param year: The year for which the data is being processed.
        :param month: The month for which the data is being processed.
        """
        output_path = f"data/analytical/{region}/{year}/analytical_data_{month}.parquet"
        df.write.mode("overwrite").parquet(output_path)
        self.logger.info("Wrote analytical data to: %s", output_path)

    def extract_month(self, file_name):
        """
        Extract month from the file name.
        :param file_name: The name of the file.
        :return: The extracted month as a string.
        """
        return os.path.basename(file_name).split("_")[2].split(".")[0]
