"""
This module contains the DataProcessor abstract class and its concrete implementations for the staging, curated, and
analytical layers. Each implementation defines the methods to read, clean, transform, and write data for that layer.
"""

from abc import ABC, abstractmethod


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
