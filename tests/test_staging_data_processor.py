"""
This module contains tests for the StagingDataProcessor class.
"""
import unittest.mock
from unittest.mock import patch
import pytest
from src.data_processing.staging_data_processor import StagingDataProcessor


@pytest.fixture
def mock_spark():
    """
    Create a MagicMock object for SparkSession.
    :return: MagicMock
    """
    return unittest.mock.MagicMock()


@pytest.fixture
def mock_logger():
    """
    Create a MagicMock object for Logger.
    :return: MagicMock
    """
    return unittest.mock.MagicMock()


@pytest.fixture
def staging_data_processor_fixture(mock_spark, mock_logger):
    """
    Create an instance of StagingDataProcessor.
    :param mock_spark: MagicMock
    :param mock_logger: MagicMock
    :return: StagingDataProcessor
    """
    return StagingDataProcessor(mock_spark, mock_logger)


def test_get_input_files(staging_data_processor_fixture):
    """
    Test get_input_files method.
    :param staging_data_processor_fixture: StagingDataProcessor
    :return: None
    """
    with patch("glob.glob", return_value=["file1.csv", "file2.csv"]):
        files = staging_data_processor_fixture.get_input_files("region", "2020")
        assert files == ["file1.csv", "file2.csv"]


def test_read_data(staging_data_processor_fixture):
    """
    Test read_data method.
    :param staging_data_processor_fixture: StagingDataProcessor
    :return: None
    """
    df = staging_data_processor_fixture.read_data("mock_file.csv")
    assert staging_data_processor_fixture.spark.read.csv.called
    assert df is not None


if __name__ == "__main__":
    pytest.main()
