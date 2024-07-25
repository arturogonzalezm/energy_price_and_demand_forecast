"""
This module contains tests for the StagingDataProcessor class.
"""
import pytest
from unittest.mock import MagicMock, patch
from src.data_processing.staging_data_processor import StagingDataProcessor


@pytest.fixture
def mock_spark():
    """
    Create a MagicMock object for SparkSession.
    :return: MagicMock
    """
    return MagicMock()


@pytest.fixture
def mock_logger():
    """
    Create a MagicMock object for Logger.
    :return: MagicMock
    """
    return MagicMock()


@pytest.fixture
def staging_data_processor(mock_spark, mock_logger):
    """
    Create an instance of StagingDataProcessor.
    :param mock_spark: MagicMock
    :param mock_logger: MagicMock
    :return: StagingDataProcessor
    """
    return StagingDataProcessor(mock_spark, mock_logger)


def test_get_input_files(staging_data_processor):
    """
    Test get_input_files method.
    :param staging_data_processor: StagingDataProcessor
    :return: None
    """
    with patch("glob.glob", return_value=["file1.csv", "file2.csv"]):
        files = staging_data_processor.get_input_files("region", "2020")
        assert files == ["file1.csv", "file2.csv"]


def test_read_data(staging_data_processor, mock_spark):
    """
    Test read_data method.
    :param staging_data_processor: StagingDataProcessor
    :param mock_spark: MagicMock
    :return: None
    """
    schema = staging_data_processor.read_data("mock_file.csv")
    assert mock_spark.read.csv.called
    assert schema is not None


if __name__ == "__main__":
    pytest.main()
