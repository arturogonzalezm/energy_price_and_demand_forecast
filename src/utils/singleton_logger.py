"""
This module provides a thread-safe singleton logger implementation.
"""

import logging
import threading


class SingletonLogger:
    """
    A thread-safe singleton class that provides a single instance of a logger object.
    """

    _instance = None
    _lock = threading.RLock()
    DEFAULT_FORMAT = "%(asctime)s - %(threadName)s - %(levelname)s - %(message)s"

    def __new__(cls, logger_name=None, log_level=logging.DEBUG, log_format=None):
        """
        Create a new instance of the SingletonLogger class if one does not already exist.
        :param logger_name:
        :param log_level:
        :param log_format:
        """
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize_logger(logger_name, log_level, log_format)
        else:
            cls._instance._update_logger(log_level, log_format)
        return cls._instance

    def _initialize_logger(self, logger_name, log_level, log_format):
        """
        Initialize the logger instance.
        :param logger_name:
        :param log_level:
        :param log_format:
        :return:
        """
        self._logger = logging.getLogger(logger_name or self.__class__.__name__)
        self._logger.setLevel(log_level)
        self._log_level = log_level
        self._log_format = log_format or self.DEFAULT_FORMAT
        self._create_handler()

    def _update_logger(self, log_level, log_format):
        """
        Update the logger instance with new log level and format.
        :param log_level:
        :param log_format:
        :return:
        """
        self._log_level = log_level
        self._log_format = log_format or self._log_format
        self._logger.setLevel(self._log_level)
        if self._logger.handlers:
            self._logger.handlers[0].setLevel(self._log_level)
            self._logger.handlers[0].setFormatter(logging.Formatter(self._log_format))

    def _create_handler(self):
        """
        Create a console handler for the logger instance.
        :return:
        """
        if not self._logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self._log_level)
            formatter = logging.Formatter(self._log_format)
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)

    def get_logger(self):
        """
        Get the logger instance.
        :return:
        """
        return self._logger
