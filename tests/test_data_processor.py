"""
This module contains unit tests for the data processing classes.
"""
import pytest

from unittest.mock import Mock, patch
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Import the classes to test
from src.data_processing.data_processor import StagingDataProcessor, CuratedDataProcessor, AnalyticalDataProcessor


@pytest.fixture(scope="module")
def spark():
    """
    Create a SparkSession object for testing.
    :return:
    """
    return SparkSession.builder.master("local[*]").appName("unit-tests").getOrCreate()


@pytest.fixture(scope="function")
def mock_logger():
    """
    Create a mock logger object for testing.
    :return:
    """
    return Mock()


@pytest.fixture(scope="function")
def sample_df(spark):
    """
    Create a sample DataFrame for testing.
    :param spark:
    :return:
    """
    schema = StructType([
        StructField("REGION", StringType(), True),
        StructField("SETTLEMENTDATE", StringType(), True),
        StructField("TOTALDEMAND", DoubleType(), True),
        StructField("RRP", DoubleType(), True),
        StructField("PERIODTYPE", StringType(), True),
    ])
    data = [
        ("VIC", "2023/01/01 00:00:00", 100.0, 50.0, "TRADE"),
        ("VIC", "2023/01/01 00:30:00", 110.0, 55.0, "TRADE"),
    ]
    return spark.createDataFrame(data, schema)


def test_staging_data_processor(spark, mock_logger, sample_df):
    """
    Test the StagingDataProcessor class.
    :param spark:
    :param mock_logger:
    :param sample_df:
    :return:
    """
    with patch("glob.glob") as mock_glob:
        mock_glob.return_value = ["test_file.csv"]

        processor = StagingDataProcessor(spark, mock_logger)

        with patch.object(processor, "read_data", return_value=sample_df):
            processor.process_data("VIC", "2023")

        mock_logger.info.assert_called()
        mock_glob.assert_called_with("data/raw/VIC/2023/*.csv")


def test_curated_data_processor(spark, mock_logger, sample_df):
    """
    Test the CuratedDataProcessor class.
    :param spark:
    :param mock_logger:
    :param sample_df:
    :return:
    """
    with patch("glob.glob") as mock_glob:
        mock_glob.return_value = ["test_file.parquet"]

        processor = CuratedDataProcessor(spark, mock_logger)

        with patch.object(processor, "read_data", return_value=sample_df):
            processor.process_data("VIC", "2023")

        mock_logger.info.assert_called()
        mock_glob.assert_called_with("data/staging/VIC/2023/*.parquet")


def test_analytical_data_processor(spark, mock_logger):
    """
    Test the AnalyticalDataProcessor class.
    :param spark:
    :param mock_logger:
    :return:
    """
    schema = StructType([
        StructField("date", TimestampType(), True),
        StructField("avg_demand", DoubleType(), True),
        StructField("avg_rrp", DoubleType(), True),
        StructField("total_demand", DoubleType(), True),
        StructField("total_rrp", DoubleType(), True),
    ])
    data = [
        (datetime(2023, 1, 1), 100.0, 50.0, 1000.0, 500.0),
        (datetime(2023, 1, 2), 110.0, 55.0, 1100.0, 550.0),
    ]
    sample_df = spark.createDataFrame(data, schema)

    with patch("glob.glob") as mock_glob:
        mock_glob.return_value = ["test_file.parquet"]

        processor = AnalyticalDataProcessor(spark, mock_logger)

        with patch.object(processor, "read_data", return_value=sample_df):
            processor.process_data("VIC", "2023")

        mock_logger.info.assert_called()
        mock_glob.assert_called_with("data/curated/VIC/2023/*.parquet")


def test_staging_data_processor_clean_data(spark, mock_logger):
    """
    Test the clean_data method of the StagingDataProcessor class.
    :param spark:
    :param mock_logger:
    :return:
    """
    processor = StagingDataProcessor(spark, mock_logger)

    input_df = spark.createDataFrame([
        ("VIC", "2023/01/01 00:00:00", 100.0, 50.0, "TRADE"),
        ("VIC", "2023/01/01 00:30:00", None, None, "TRADE"),
    ], ["REGION", "SETTLEMENTDATE", "TOTALDEMAND", "RRP", "PERIODTYPE"])

    cleaned_df = processor.clean_data(input_df)

    assert cleaned_df.count() == 2
    assert cleaned_df.filter(cleaned_df.TOTALDEMAND.isNull()).count() == 0
    assert cleaned_df.filter(cleaned_df.RRP.isNull()).count() == 0


def test_curated_data_processor_transform_data(spark, mock_logger):
    """
    Test the transform_data method of the CuratedDataProcessor class.
    :param spark:
    :param mock_logger:
    :return:
    """
    processor = CuratedDataProcessor(spark, mock_logger)

    input_df = spark.createDataFrame([
        (datetime(2023, 1, 1), 100.0, 50.0, "TRADE"),
        (datetime(2023, 1, 1), 110.0, 55.0, "TRADE"),
    ], ["date", "TOTALDEMAND", "RRP", "PERIODTYPE"])

    transformed_df = processor.transform_data(input_df)

    assert transformed_df.count() == 1
    row = transformed_df.collect()[0]
    assert row.avg_demand == 105.0
    assert row.avg_rrp == 52.5
    assert row.total_demand == 210.0
    assert row.total_rrp == 105.0


def test_analytical_data_processor_transform_data(spark, mock_logger):
    """
    Test the transform_data method of the AnalyticalDataProcessor class.
    :param spark:
    :param mock_logger:
    :return:
    """
    processor = AnalyticalDataProcessor(spark, mock_logger)

    input_df = spark.createDataFrame([
        (datetime(2023, 1, 1), 100.0, 50.0, 1000.0, 500.0),
        (datetime(2023, 1, 1), 110.0, 55.0, 1100.0, 550.0),
    ], ["date", "avg_demand", "avg_rrp", "total_demand", "total_rrp"])

    transformed_df = processor.transform_data(input_df)

    assert transformed_df.count() == 1
    row = transformed_df.collect()[0]
    assert row.monthly_avg_demand == 105.0
    assert row.monthly_avg_rrp == 52.5
    assert row.monthly_total_demand == 2100.0
    assert row.monthly_total_rrp == 1050.0


if __name__ == "__main__":
    pytest.main()
