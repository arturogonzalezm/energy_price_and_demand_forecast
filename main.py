"""
Main module to run data processing for staging, curated, and analytical layers.
"""
from src.data_processing.analytical_data_processor import AnalyticalDataProcessor
from src.data_processing.curated_data_processor import CuratedDataProcessor
from src.data_processing.staging_data_processor import StagingDataProcessor
from src.utils.singleton_logger import SingletonLogger
from src.utils.spark_session import SparkSessionManager


def main():
    """
    Main function to orchestrate data processing.
    """
    spark = SparkSessionManager.get_instance()
    logger = SingletonLogger().get_logger()

    regions = ["NSW", "VIC", "QLD", "TAS", "SA"]
    year = "2022"

    processors = [
        StagingDataProcessor(spark, logger),
        CuratedDataProcessor(spark, logger),
        AnalyticalDataProcessor(spark, logger)
    ]

    for processor in processors:
        logger.info("Starting %s", processor.__class__.__name__)
        for region in regions:
            logger.info("Processing region: %s", region)
            processor.process_data(region, year)
        logger.info("Completed %s", processor.__class__.__name__)

    logger.info("All processing completed")


if __name__ == "__main__":
    main()
