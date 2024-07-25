"""
This module contains the tests for the CuratedDataProcessor class.
"""
import unittest.mock
from unittest.mock import patch
import pytest
from src.data_processing.curated_data_processor import CuratedDataProcessor


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
def curated_data_processor_fixture(mock_spark, mock_logger):
    """
    Create an instance of CuratedDataProcessor.
    :param mock_spark: MagicMock
    :param mock_logger: MagicMock
    :return: CuratedDataProcessor
    """
    return CuratedDataProcessor(mock_spark, mock_logger)


def test_get_input_files(curated_data_processor_fixture):
    """
    Test get_input_files method.
    :param curated_data_processor_fixture: CuratedDataProcessor
    :return: None
    """
    with patch("glob.glob", return_value=["file1.parquet", "file2.parquet"]):
        files = curated_data_processor_fixture.get_input_files("region", "2020")
        assert files == ["file1.parquet", "file2.parquet"]


def test_read_data(curated_data_processor_fixture):
    """
    Test read_data method.
    :param curated_data_processor_fixture: CuratedDataProcessor
    :return: None
    """
    df = curated_data_processor_fixture.read_data("mock_file.parquet")
    assert curated_data_processor_fixture.spark.read.parquet.called
    assert df is not None


if __name__ == "__main__":
    pytest.main()
