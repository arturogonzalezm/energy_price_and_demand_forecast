"""
Test the SingletonLogger class.
"""
import logging
import threading
import pytest
import unittest.mock
from src.utils.singleton_logger import SingletonLogger


@pytest.fixture
def reset_singleton():
    """Reset the singleton instance before each test."""
    SingletonLogger._instance = None
    yield
    SingletonLogger._instance = None


def test_singleton_instance(reset_singleton):
    """Test that multiple instantiations return the same instance."""
    logger1 = SingletonLogger()
    logger2 = SingletonLogger()
    assert logger1 is logger2


def test_logger_name(reset_singleton):
    """Test that the logger name is set correctly."""
    logger = SingletonLogger(logger_name="TestLogger").get_logger()
    assert logger.name == "TestLogger"


def test_default_logger_name(reset_singleton):
    """Test that the default logger name is set correctly."""
    logger = SingletonLogger().get_logger()
    assert logger.name == "SingletonLogger"


def test_format_update(reset_singleton):
    """Test that the format is updated when a new instance is created with a different format."""
    SingletonLogger()  # Create with default format
    custom_format = "%(levelname)s: %(message)s"
    logger = SingletonLogger(log_format=custom_format).get_logger()
    assert logger.handlers[0].formatter._fmt == custom_format


def test_level_update(reset_singleton):
    """Test that the level is updated when a new instance is created with a different level."""
    SingletonLogger(log_level=logging.DEBUG)  # Create with DEBUG level
    logger = SingletonLogger(log_level=logging.INFO).get_logger()
    assert logger.level == logging.INFO
    assert logger.handlers[0].level == logging.INFO


def test_single_handler(reset_singleton):
    """Test that only one handler is added, even with multiple instantiations."""
    logger1 = SingletonLogger().get_logger()
    logger2 = SingletonLogger().get_logger()
    assert len(logger1.handlers) == 1
    assert len(logger2.handlers) == 1
    assert logger1.handlers[0] is logger2.handlers[0]


def test_thread_safety(reset_singleton):
    """Test thread safety of the singleton pattern."""

    def create_logger():
        return SingletonLogger()

    threads = [threading.Thread(target=create_logger) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    loggers = [SingletonLogger() for _ in range(10)]
    assert all(logger is loggers[0] for logger in loggers)


def test_logger_functionality(reset_singleton):
    """Test that the logger actually logs messages."""
    logger = SingletonLogger().get_logger()
    with unittest.mock.patch.object(logger, 'debug') as mock_debug:
        logger.debug("Test message")
        mock_debug.assert_called_once_with("Test message")


if __name__ == "__main__":
    pytest.main()
