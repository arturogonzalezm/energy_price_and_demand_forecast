"""
This module contains the tests for the CuratedDataProcessor class.
"""
import pytest
from unittest.mock import MagicMock, patch
from src.data_processing.curated_data_processor import CuratedDataProcessor


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
def curated_data_processor(mock_spark, mock_logger):
    """
    Create an instance of CuratedDataProcessor.
    :param mock_spark: MagicMock
    :param mock_logger: MagicMock
    :return: CuratedDataProcessor
    """
    return CuratedDataProcessor(mock_spark, mock_logger)


def test_get_input_files(curated_data_processor):
    """
    Test get_input_files method.
    :param curated_data_processor: CuratedDataProcessor
    :return: None
    """
    with patch("glob.glob", return_value=["file1.parquet", "file2.parquet"]):
        files = curated_data_processor.get_input_files("region", "2020")
        assert files == ["file1.parquet", "file2.parquet"]


def test_read_data(curated_data_processor, mock_spark):
    """
    Test read_data method.
    :param curated_data_processor: CuratedDataProcessor
    :param mock_spark: MagicMock
    :return: None
    """
    df = curated_data_processor.read_data("mock_file.parquet")
    assert mock_spark.read.parquet.called
    assert df is not None


if __name__ == "__main__":
    pytest.main()
