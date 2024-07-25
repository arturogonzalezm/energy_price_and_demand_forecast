"""
This module contains the tests for the AnalyticalDataProcessor class.
"""
import pytest
from unittest.mock import MagicMock, patch

from pyspark.sql.functions import col

from src.data_processing.analytical_data_processor import AnalyticalDataProcessor
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import Row


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
    Fixture for creating an AnalyticalDataProcessor instance.
    :param mock_spark: The mock Spark session.
    :param mock_logger: The mock logger.
    :return: An instance of AnalyticalDataProcessor.
    """
    return AnalyticalDataProcessor(mock_spark, mock_logger)


def test_get_input_files(analytical_data_processor):
    """
    Test the get_input_files method.
    :param analytical_data_processor: The AnalyticalDataProcessor instance.
    """
    with patch("glob.glob", return_value=["file1.parquet", "file2.parquet"]):
        files = analytical_data_processor.get_input_files("region", "2020")
        assert files == ["file1.parquet", "file2.parquet"]


def test_read_data(analytical_data_processor):
    """
    Test the read_data method.
    :param analytical_data_processor: The AnalyticalDataProcessor instance.
    """
    mock_spark_session = MagicMock()
    analytical_data_processor.spark = mock_spark_session

    mock_df = MagicMock()
    mock_spark_session.read.parquet.return_value = mock_df

    df = analytical_data_processor.read_data("mock_file.parquet")
    mock_spark_session.read.parquet.assert_called_once_with("mock_file.parquet")
    assert df is mock_df


def test_clean_data(analytical_data_processor, mock_spark):
    """
    Test the clean_data method.
    :param analytical_data_processor: The AnalyticalDataProcessor instance.
    :param mock_spark: The mock Spark session.
    """
    df = mock_spark.createDataFrame([("2020-01-01", 100.0)], ["date", "value"])
    cleaned_df = analytical_data_processor.clean_data(df)
    assert cleaned_df is df  # Data should be returned as is


def test_transform_data(analytical_data_processor):
    """
    Test the transform_data method.
    :param analytical_data_processor: The AnalyticalDataProcessor instance.
    """
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("avg_demand", DoubleType(), True),
        StructField("avg_rrp", DoubleType(), True),
        StructField("total_demand", DoubleType(), True),
        StructField("total_rrp", DoubleType(), True)
    ])
    data = [
        Row(date="2020-01-01", avg_demand=100.0, avg_rrp=50.0, total_demand=1000.0, total_rrp=500.0),
    ]
    spark = SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()
    df = spark.createDataFrame(data, schema)

    # Correctly invoke the transform_data method
    transformed_df = analytical_data_processor.transform_data(df)

    # Debug: Show the transformed DataFrame schema and data
    transformed_df.printSchema()
    transformed_df.show()

    assert "monthly_avg_demand" in transformed_df.columns
    assert "monthly_avg_rrp" in transformed_df.columns


# def test_feature_engineering(analytical_data_processor, sample_df):
#     """
#     Test the feature_engineering method, including anomaly detection.
#     :param analytical_data_processor: The AnalyticalDataProcessor instance.
#     :param sample_df: The sample DataFrame.
#     """
#     result_df = analytical_data_processor.feature_engineering(sample_df)
#
#     # Check if all expected columns are present
#     expected_columns = [
#         "date", "monthly_total_demand", "monthly_total_rrp", "demand_rrp_ratio",
#         "mean_ratio", "stddev_ratio", "z_score", "is_anomaly"
#     ]
#     assert set(result_df.columns) == set(expected_columns)
#
#     # Check the number of rows
#     assert result_df.count() == 5
#
#     # Check demand_rrp_ratio calculation
#     first_row = result_df.filter(col("date") == "2023-01-01").first()
#     assert pytest.approx(first_row["demand_rrp_ratio"], 0.01) == 2.0
#
#     # Check z_score calculation
#     april_row = result_df.filter(col("date") == "2023-04-01").first()
#     assert abs(april_row["z_score"]) > 3  # This should be our anomaly
#
#     # Check is_anomaly flag
#     assert april_row["is_anomaly"] is True
#     assert result_df.filter(col("is_anomaly") == True).count() == 1


def test_write_data(analytical_data_processor, mock_spark):
    """
    Test the write_data method.
    :param analytical_data_processor: The AnalyticalDataProcessor instance.
    :param mock_spark: The mock Spark session.
    """
    df = mock_spark.createDataFrame([("2020-01-01", 100.0)], ["date", "value"])
    with patch.object(df.write, 'mode') as mock_write_mode:
        mock_write = MagicMock()
        mock_write_mode.return_value = mock_write
        analytical_data_processor.write_data(df, "region", "2020", "01")
        mock_write_mode.assert_called_once_with("overwrite")
        mock_write.parquet.assert_called_once_with("data/analytical/region/2020/analytical_data_01.parquet")


def test_extract_month(analytical_data_processor):
    """
    Test the extract_month method.
    :param analytical_data_processor: The AnalyticalDataProcessor instance.
    """
    month = analytical_data_processor.extract_month("analytical_data_202003.parquet")
    assert month == "202003"


if __name__ == "__main__":
    pytest.main()
