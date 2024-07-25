"""
This module contains the tests for the AnalyticalDataProcessor class.
"""
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.data_processing.analytical_data_processor import AnalyticalDataProcessor


@pytest.fixture(scope="module")
def spark():
    """
    Fixture for creating a Spark session for testing.

    Returns:
        SparkSession: A Spark session for testing.
    """
    return SparkSession.builder.master("local").appName("UnitTest").getOrCreate()


@pytest.fixture
def logger():
    """
    Fixture for creating a mock logger.

    Returns:
        MagicMock: A mock logger instance.
    """
    return MagicMock()


@pytest.fixture
def processor(spark, logger):
    """
    Fixture for creating an AnalyticalDataProcessor instance.

    Args:
        spark (SparkSession): The Spark session fixture.
        logger (MagicMock): The logger fixture.

    Returns:
        AnalyticalDataProcessor: An instance of AnalyticalDataProcessor.
    """
    return AnalyticalDataProcessor(spark, logger)


def test_get_input_files(processor):
    """
    Test the get_input_files method.

    Args:
        processor (AnalyticalDataProcessor): The processor fixture.
    """
    with patch("src.data_processing.analytical_data_processor.glob.glob") as mock_glob:
        mock_glob.return_value = ["file1.parquet", "file2.parquet"]
        files = processor.get_input_files("region1", 2023)
        assert files == ["file1.parquet", "file2.parquet"]


def test_read_data(processor):
    """
    Test the read_data method.

    Args:
        processor (AnalyticalDataProcessor): The processor fixture.
    """
    mock_df = MagicMock()
    with patch("pyspark.sql.readwriter.DataFrameReader.parquet", return_value=mock_df):
        df = processor.read_data("file1.parquet")
        assert df == mock_df


def test_clean_data(processor):
    """
    Test the clean_data method.

    Args:
        processor (AnalyticalDataProcessor): The processor fixture.
    """
    mock_df = MagicMock()
    cleaned_df = processor.clean_data(mock_df)
    assert cleaned_df == mock_df


def test_transform_data(processor, spark):
    """
    Test the transform_data method.

    Args:
        processor (AnalyticalDataProcessor): The processor fixture.
        spark (SparkSession): The Spark session fixture.
    """
    mock_df = spark.createDataFrame(
        [("2021-01", 10, 20, 100, 200), ("2021-01", 15, 25, 150, 250), ("2021-02", 20, 30, 200, 300)],
        ["date", "avg_demand", "avg_rrp", "total_demand", "total_rrp"]
    )
    transformed_df = processor.transform_data(mock_df)
    transformed_df.show()
    assert "monthly_avg_demand" in transformed_df.columns
    assert "monthly_avg_rrp" in transformed_df.columns
    assert "monthly_total_demand" in transformed_df.columns
    assert "monthly_total_rrp" in transformed_df.columns


def test_feature_engineering(processor, spark):
    """
    Test the feature_engineering method.

    Args:
        processor (AnalyticalDataProcessor): The processor fixture.
        spark (SparkSession): The Spark session fixture.
    """
    mock_df = spark.createDataFrame(
        [("2021-01", 100, 200), ("2021-02", 150, 250)],
        ["date", "monthly_total_demand", "monthly_total_rrp"]
    )
    engineered_df = processor.feature_engineering(mock_df)
    engineered_df.show()
    assert "demand_rrp_ratio" in engineered_df.columns
    assert "mean_ratio" in engineered_df.columns
    assert "stddev_ratio" in engineered_df.columns
    assert "z_score" in engineered_df.columns
    assert "is_anomaly" in engineered_df.columns


def test_write_data(processor, spark):
    """
    Test the write_data method.

    Args:
        processor (AnalyticalDataProcessor): The processor fixture.
        spark (SparkSession): The Spark session fixture.
    """
    mock_df = spark.createDataFrame(
        [("2021-01", 100, 200, 0.5, 0.5, 0.1, 0, False)],
        ["date", "monthly_total_demand", "monthly_total_rrp", "demand_rrp_ratio", "mean_ratio", "stddev_ratio",
         "z_score", "is_anomaly"]
    )
    with patch("src.data_processing.analytical_data_processor.AnalyticalDataProcessor.write_data") as mock_write:
        processor.write_data(mock_df, "region1", 2023, "01")
        mock_write.assert_called_once()


def test_extract_month(processor):
    """
    Test the extract_month method.

    Args:
        processor (AnalyticalDataProcessor): The processor fixture.
    """
    month = processor.extract_month("analytical_data_202006.parquet")
    assert month == "202006"


@pytest.mark.parametrize("region, year, expected", [
    ("region1", 2023, ["file1.parquet", "file2.parquet"]),
    ("region2", 2023, ["file3.parquet"]),
    ("region3", 2022, []),
])
def test_get_input_files_param(processor, region, year, expected):
    """
    Test the get_input_files method with parameterized inputs.

    Args:
        processor (AnalyticalDataProcessor): The processor fixture.
        region (str): The region for the input files.
        year (int): The year for the input files.
        expected (list): The expected list of input files.
    """
    with patch("src.data_processing.analytical_data_processor.glob.glob", return_value=expected):
        files = processor.get_input_files(region, year)
        assert files == expected


if __name__ == "__main__":
    pytest.main()
