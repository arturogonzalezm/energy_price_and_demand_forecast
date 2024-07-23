"""
This module is responsible for creating a singleton instance of SparkSession.
"""
from pyspark.sql import SparkSession


class SparkSessionManager:
    """
    This class is responsible for creating a singleton instance of SparkSession.
    """
    _instance = None

    @classmethod
    def get_instance(cls):
        """
        Get the singleton instance of SparkSession.
        :return: The singleton instance of SparkSession
        :rtype: SparkSession
        """
        if cls._instance is None:
            cls._instance = SparkSession.builder.appName("EmployeeBoss").getOrCreate()
        return cls._instance

    @classmethod
    def stop_instance(cls):
        """
        Stop the singleton instance of SparkSession.
        :return: None
        :rtype: None
        """
        if cls._instance:
            cls._instance.stop()
            cls._instance = None
