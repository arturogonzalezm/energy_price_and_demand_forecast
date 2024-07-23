"""
Unit tests for the SparkSessionManager class.
"""
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

from src.utils.spark_session import SparkSessionManager


@pytest.fixture(autouse=True)
def reset_spark_session_manager():
    """
    Reset the SparkSessionManager before and after each test.
    :return: None
    """
    SparkSessionManager._instance = None
    yield
    SparkSessionManager._instance = None


def test_get_instance_creates_new_session():
    """
    Test that get_instance creates a new SparkSession instance.
    :return: None
    """
    with patch('src.utils.spark_session.SparkSession') as mock_spark_session:
        mock_builder = MagicMock()
        mock_spark_session.builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock(spec=SparkSession)

        session = SparkSessionManager.get_instance()

        mock_spark_session.builder.appName.assert_called_once_with("DefaultApp")
        mock_builder.getOrCreate.assert_called_once()
        assert isinstance(session, MagicMock)
        assert session == SparkSessionManager._instance


def test_get_instance_returns_existing_session():
    """
    Test that get_instance returns an existing SparkSession instance.
    :return: None
    """
    with patch('src.utils.spark_session.SparkSession') as mock_spark_session:
        # Create an initial session
        initial_session = SparkSessionManager.get_instance()

        # Reset the mock to check it's not called again
        mock_spark_session.builder.appName.reset_mock()

        # Get the instance again
        second_session = SparkSessionManager.get_instance()

        assert second_session == initial_session
        mock_spark_session.builder.appName.assert_not_called()


def test_stop_instance():
    """
    Test that stop_instance stops the SparkSession instance.
    :return: None
    """
    with patch('src.utils.spark_session.SparkSession') as mock_spark_session:
        # Create a session
        session = SparkSessionManager.get_instance()
        original_stop = session.stop
        stop_called = [False]

        def mock_stop():
            stop_called[0] = True

        session.stop = mock_stop

        # Stop the session
        SparkSessionManager.stop_instance()

        assert stop_called[0]
        assert SparkSessionManager._instance is None

        # Restore original method
        session.stop = original_stop


def test_stop_instance_no_existing_session():
    """
    Test that stop_instance does not raise an error when no session exists.
    :return: None
    """
    SparkSessionManager._instance = None

    # Stopping when no session exists should not raise an error
    SparkSessionManager.stop_instance()

    assert SparkSessionManager._instance is None


def test_multiple_get_instance_calls():
    """
    Test that multiple calls to get_instance return the same instance.
    :return: None
    """
    with patch('src.utils.spark_session.SparkSession') as mock_spark_session:
        mock_builder = MagicMock()
        mock_spark_session.builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock(spec=SparkSession)

        # Get instance multiple times
        session1 = SparkSessionManager.get_instance()
        session2 = SparkSessionManager.get_instance()
        session3 = SparkSessionManager.get_instance()

        # Verify that SparkSession.builder.appName().getOrCreate() was called only once
        mock_spark_session.builder.appName.assert_called_once_with("DefaultApp")
        mock_builder.getOrCreate.assert_called_once()

        # Verify that all calls return the same instance
        assert session1 == session2 == session3


if __name__ == "__main__":
    pytest.main()
