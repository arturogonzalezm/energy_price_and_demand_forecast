"""
This module contains the tests for the AnalyticalDataProcessor class.
"""
import unittest.mock
from unittest.mock import patch, MagicMock
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import Row
from pyspark.sql.functions import col
from src.data_processing.analytical_data_processor import AnalyticalDataProcessor


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    :return: SparkSession
    """
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


@pytest.fixture
def mock_logger():
    """
    Create a MagicMock object for Logger.
    :return: MagicMock
    """
    return MagicMock()


@pytest.fixture
def analytical_data_processor_fixture(spark, mock_logger):
    """
    Fixture for creating an AnalyticalDataProcessor instance.
    :param spark: The Spark session.
    :param mock_logger: The mock logger.
    :return: An instance of AnalyticalDataProcessor.
    """
    return AnalyticalDataProcessor(spark, mock_logger)


@pytest.fixture
def sample_df(spark):
    """
    Fixture for creating a sample DataFrame.
    :param spark: The Spark session.
    :return: DataFrame
    """
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("monthly_total_demand", DoubleType(), True),
        StructField("monthly_total_rrp", DoubleType(), True),
        StructField("demand_rrp_ratio", DoubleType(), True)
    ])
    data = [
        Row(date="2023-01-01", monthly_total_demand=1000.0, monthly_total_rrp=500.0, demand_rrp_ratio=2.0),
        Row(date="2023-02-01", monthly_total_demand=1100.0, monthly_total_rrp=550.0, demand_rrp_ratio=2.0),
        Row(date="2023-03-01", monthly_total_demand=900.0, monthly_total_rrp=450.0, demand_rrp_ratio=2.0),
        Row(date="2023-04-01", monthly_total_demand=3000.0, monthly_total_rrp=100.0, demand_rrp_ratio=30.0),  # anomaly
        Row(date="2023-05-01", monthly_total_demand=950.0, monthly_total_rrp=475.0, demand_rrp_ratio=2.0)
    ]
    return spark.createDataFrame(data, schema)


def test_get_input_files(analytical_data_processor_fixture):
    """
    Test the get_input_files method.
    :param analytical_data_processor_fixture: The AnalyticalDataProcessor instance.
    """
    with patch("glob.glob", return_value=["file1.parquet", "file2.parquet"]):
        files = analytical_data_processor_fixture.get_input_files("region", "2020")
        assert files == ["file1.parquet", "file2.parquet"]


def test_read_data(analytical_data_processor_fixture):
    """
    Test the read_data method.
    :param analytical_data_processor_fixture: The AnalyticalDataProcessor instance.
    """
    mock_spark_session = MagicMock()
    analytical_data_processor_fixture.spark = mock_spark_session

    mock_df = MagicMock()
    mock_spark_session.read.parquet.return_value = mock_df

    df = analytical_data_processor_fixture.read_data("mock_file.parquet")
    mock_spark_session.read.parquet.assert_called_once_with("mock_file.parquet")
    assert df is mock_df


def test_clean_data(analytical_data_processor_fixture, spark):
    """
    Test the clean_data method.
    :param analytical_data_processor_fixture: The AnalyticalDataProcessor instance.
    :param spark: The Spark session.
    """
    df = spark.createDataFrame([("2020-01-01", 100.0)], ["date", "value"])
    cleaned_df = analytical_data_processor_fixture.clean_data(df)
    assert cleaned_df is df  # Data should be returned as is


def test_transform_data(analytical_data_processor_fixture, spark):
    """
    Test the transform_data method.
    :param analytical_data_processor_fixture: The AnalyticalDataProcessor instance.
    :param spark: The Spark session.
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
    df = spark.createDataFrame(data, schema)

    transformed_df = analytical_data_processor_fixture.transform_data(df)

    transformed_df.printSchema()
    transformed_df.show()

    assert "monthly_avg_demand" in transformed_df.columns
    assert "monthly_avg_rrp" in transformed_df.columns


# def test_feature_engineering(analytical_data_processor_fixture, sample_df):
#     """
#     Test the feature_engineering method, including anomaly detection.
#     :param analytical_data_processor_fixture: The AnalyticalDataProcessor instance.
#     :param sample_df: The sample DataFrame.
#     """
#     result_df = analytical_data_processor_fixture.feature_engineering(sample_df)
#
#     expected_columns = [
#         "date", "monthly_total_demand", "monthly_total_rrp", "demand_rrp_ratio",
#         "mean_ratio", "stddev_ratio", "z_score", "is_anomaly"
#     ]
#     assert set(result_df.columns) == set(expected_columns)
#
#     assert result_df.count() == 5
#
#     first_row = result_df.filter(col("date") == "2023-01-01").first()
#     assert pytest.approx(first_row["demand_rrp_ratio"], 0.01) == 2.0
#
#     april_row = result_df.filter(col("date") == "2023-04-01").first()
#     print("Debug - April row values:", april_row)
#     print("Debug - April z_score:", april_row["z_score"])
#     print("Debug - mean_ratio and stddev_ratio:", result_df.select("mean_ratio", "stddev_ratio").distinct().collect())
#
#     assert abs(april_row["z_score"]) > 3  # This should be our anomaly
#
#     assert april_row["is_anomaly"] is True
#     assert result_df.filter(col("is_anomaly") == True).count() == 1


# def test_write_data(analytical_data_processor_fixture, spark):
#     """
#     Test the write_data method.
#     :param analytical_data_processor_fixture: The AnalyticalDataProcessor instance.
#     :param spark: The Spark session.
#     """
#     df = spark.createDataFrame([("2020-01-01", 100.0)], ["date", "value"])
#     with patch.object(df.write, 'mode') as mock_write_mode:
#         mock_write = MagicMock()
#         mock_write_mode.return_value = mock_write
#         analytical_data_processor_fixture.write_data(df, "region", "2020", "01")
#         mock_write_mode.assert_called_once_with("overwrite")
#         mock_write.parquet.assert_called_once_with("data/analytical/region/2020/analytical_data_01.parquet")


def test_extract_month(analytical_data_processor_fixture):
    """
    Test the extract_month method.
    :param analytical_data_processor_fixture: The AnalyticalDataProcessor instance.
    """
    month = analytical_data_processor_fixture.extract_month("analytical_data_202003.parquet")
    assert month == "202003"


if __name__ == "__main__":
    pytest.main()
