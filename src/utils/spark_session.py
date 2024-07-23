"""
This module is responsible for creating and managing a singleton instance of SparkSession.
"""
from pyspark.sql import SparkSession


class SparkSessionManager:
    """
    This class is responsible for creating and managing a singleton instance of SparkSession.
    """
    _instance = None

    @classmethod
    def get_instance(cls, app_name="DefaultApp"):
        """
        Get the singleton instance of SparkSession.
        :param app_name: Name of the Spark application
        :type app_name: str
        :return: The singleton instance of SparkSession
        :rtype: SparkSession
        """
        if cls._instance is None:
            cls._instance = SparkSession.builder.appName(app_name).getOrCreate()
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
