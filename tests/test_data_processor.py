"""
This module contains tests for the DataProcessor class.
"""

import pytest
from unittest.mock import MagicMock
from src.data_processing.data_processor import DataProcessor


class MockDataProcessor(DataProcessor):
    """
    A mock implementation of the DataProcessor abstract class for testing purposes.
    """

    def get_input_files(self, region, year):
        """
        Get list of input files to process.
        :param region: The region for which to get input files.
        :param year: The year for which to get input files.
        :return: A list of input file paths.
        """
        return ["mock_file.csv"]

    def read_data(self, input_file):
        """
        Read data from the input file.
        :param input_file: The path to the input file.
        :return: A mock DataFrame object.
        """
        return MagicMock()

    def clean_data(self, df):
        """
        Clean the input data.
        :param df: The input DataFrame.
        :return: The cleaned DataFrame.
        """
        return df

    def transform_data(self, df):
        """
        Transform the cleaned data.
        :param df: The cleaned DataFrame.
        :return: The transformed DataFrame.
        """
        return df

    def feature_engineering(self, df):
        """
        Perform feature engineering on the transformed data.
        :param df: The transformed DataFrame.
        :return: The DataFrame with additional features.
        """
        return df

    def write_data(self, df, region, year, month):
        """
        Write the processed data.
        :param df: The DataFrame to write.
        :param region: The region for which the data is being processed.
        :param year: The year for which the data is being processed.
        :param month: The month for which the data is being processed.
        """
        pass

    def extract_month(self, file_name):
        """
        Extract month from the file name.
        :param file_name: The name of the file.
        :return: The extracted month as a string.
        """
        return "202001"


@pytest.fixture
def mock_spark():
    """
    Fixture for creating a mock Spark session.
    :return: A mock Spark session object.
    """
    return MagicMock()


@pytest.fixture
def mock_logger():
    """
    Fixture for creating a mock logger.
    :return: A mock logger object.
    """
    return MagicMock()


@pytest.fixture
def data_processor(mock_spark, mock_logger):
    """
    Fixture for creating a MockDataProcessor instance.
    :param mock_spark: The mock Spark session.
    :param mock_logger: The mock logger.
    :return: An instance of MockDataProcessor.
    """
    return MockDataProcessor(mock_spark, mock_logger)


def test_process_data_no_files(data_processor, mock_logger):
    """
    Test the process_data method when no input files are found.
    :param data_processor: The MockDataProcessor instance.
    :param mock_logger: The mock logger.
    """
    data_processor.get_input_files = MagicMock(return_value=[])
    data_processor.process_data("region", "2020")
    mock_logger.warning.assert_called_with("No input files found for %s in %s", "region", "2020")


def test_process_data_with_files(data_processor, mock_logger):
    """
    Test the process_data method when input files are found.
    :param data_processor: The MockDataProcessor instance.
    :param mock_logger: The mock logger.
    """
    df_mock = MagicMock()
    df_mock.count.side_effect = [10, 8, 7, 7]

    data_processor.read_data = MagicMock(return_value=df_mock)
    data_processor.clean_data = MagicMock(return_value=df_mock)
    data_processor.transform_data = MagicMock(return_value=df_mock)
    data_processor.feature_engineering = MagicMock(return_value=df_mock)

    data_processor.process_data("region", "2020")

    mock_logger.info.assert_any_call("Found %d files to process for %s in %s", 1, "region", "2020")
    mock_logger.info.assert_any_call("Read %d rows from %s", 10, "mock_file.csv")
    mock_logger.info.assert_any_call("Cleaned data, now have %d rows", 8)
    mock_logger.info.assert_any_call("Transformed data, now have %d rows", 7)
    mock_logger.info.assert_any_call("Feature engineered data, now have %d rows", 7)
    mock_logger.info.assert_any_call("Completed processing %s", "mock_file.csv")


if __name__ == "__main__":
    pytest.main()
