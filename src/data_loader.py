from pyspark.sql import DataFrame
from typing import List
from src.utils.spark_session import SparkSessionManager


class DataLoader:
    @staticmethod
    def read_csv_files(file_paths: List[str]) -> DataFrame:
        """
        Read multiple CSV files into a single DataFrame.
        :param file_paths: List of file paths to read
        :return: Combined DataFrame
        """
        spark = SparkSessionManager.get_instance()
        dataframes = [spark.read.csv(file_path, header=True, inferSchema=True) for file_path in file_paths]
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.union(df)
        return combined_df

    @staticmethod
    def read_parquet_files(file_paths: List[str]) -> DataFrame:
        """
        Read multiple Parquet files into a single DataFrame.
        :param file_paths: List of file paths to read
        :return: Combined DataFrame
        """
        spark = SparkSessionManager.get_instance()
        dataframes = [spark.read.parquet(file_path) for file_path in file_paths]
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.union(df)
        return combined_df
