"""
This module contains the tests for the AnalyticalDataProcessor class.
"""
import pytest
from unittest.mock import MagicMock, patch
from src.data_processing.analytical_data_processor import AnalyticalDataProcessor


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
def analytical_data_processor(mock_spark, mock_logger):
    """
    Create an instance of AnalyticalDataProcessor.
    :param mock_spark: MagicMock
    :param mock_logger: MagicMock
    :return: AnalyticalDataProcessor
    """
    return AnalyticalDataProcessor(mock_spark, mock_logger)


def test_get_input_files(analytical_data_processor):
    """
    Test get_input_files method.
    :param analytical_data_processor: AnalyticalDataProcessor
    :return: None
    """
    with patch("glob.glob", return_value=["file1.parquet", "file2.parquet"]):
        files = analytical_data_processor.get_input_files("region", "2020")
        assert files == ["file1.parquet", "file2.parquet"]


def test_read_data(analytical_data_processor, mock_spark):
    """
    Test read_data method.
    :param analytical_data_processor: AnalyticalDataProcessor
    :param mock_spark: MagicMock
    :return: None
    """
    df = analytical_data_processor.read_data("mock_file.parquet")
    assert mock_spark.read.parquet.called
    assert df is not None


if __name__ == "__main__":
    pytest.main()
