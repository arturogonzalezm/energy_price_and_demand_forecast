import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame

from src.data_loader import DataLoader
from src.utils.spark_session import SparkSessionManager


@pytest.fixture
def mock_spark_session():
    with patch.object(SparkSessionManager, 'get_instance') as mock_get_instance:
        mock_spark = MagicMock()
        mock_get_instance.return_value = mock_spark
        yield mock_spark


def create_mock_dataframe(data):
    df = MagicMock(spec=DataFrame)
    df.union = MagicMock(return_value=df)
    return df


class TestDataLoader:

    def test_read_csv_files(self, mock_spark_session):
        # Arrange
        file_paths = ['file1.csv', 'file2.csv', 'file3.csv']
        mock_dataframes = [create_mock_dataframe(f"data{i}") for i in range(len(file_paths))]
        mock_spark_session.read.csv.side_effect = mock_dataframes

        # Act
        result = DataLoader.read_csv_files(file_paths)

        # Assert
        assert mock_spark_session.read.csv.call_count == len(file_paths)
        for file_path in file_paths:
            mock_spark_session.read.csv.assert_any_call(file_path, header=True, inferSchema=True)

        assert result == mock_dataframes[0]
        assert mock_dataframes[0].union.call_count == len(file_paths) - 1

    def test_read_parquet_files(self, mock_spark_session):
        # Arrange
        file_paths = ['file1.parquet', 'file2.parquet', 'file3.parquet']
        mock_dataframes = [create_mock_dataframe(f"data{i}") for i in range(len(file_paths))]
        mock_spark_session.read.parquet.side_effect = mock_dataframes

        # Act
        result = DataLoader.read_parquet_files(file_paths)

        # Assert
        assert mock_spark_session.read.parquet.call_count == len(file_paths)
        for file_path in file_paths:
            mock_spark_session.read.parquet.assert_any_call(file_path)

        assert result == mock_dataframes[0]
        assert mock_dataframes[0].union.call_count == len(file_paths) - 1

    def test_read_csv_files_empty_list(self, mock_spark_session):
        # Arrange
        file_paths = []

        # Act & Assert
        with pytest.raises(IndexError):
            DataLoader.read_csv_files(file_paths)

    def test_read_parquet_files_empty_list(self, mock_spark_session):
        # Arrange
        file_paths = []

        # Act & Assert
        with pytest.raises(IndexError):
            DataLoader.read_parquet_files(file_paths)


if __name__ == "__main__":
    pytest.main()
