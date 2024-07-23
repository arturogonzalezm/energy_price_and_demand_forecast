from src.data_processing.data_processor import StagingDataProcessor, CuratedDataProcessor, AnalyticalDataProcessor
from src.utils.singleton_logger import SingletonLogger
from src.utils.spark_session import SparkSessionManager


def main():
    spark = SparkSessionManager.get_instance()
    logger = SingletonLogger().get_logger()

    regions = ["NSW", "VIC", "QLD", "TAS", "SA"]
    year = "2021"

    processors = [
        StagingDataProcessor(spark, logger),
        CuratedDataProcessor(spark, logger),
        AnalyticalDataProcessor(spark, logger)
    ]

    for processor in processors:
        logger.info(f"Starting {processor.__class__.__name__}")
        for region in regions:
            logger.info(f"Processing region: {region}")
            processor.process_data(region, year)
        logger.info(f"Completed {processor.__class__.__name__}")

    logger.info("All processing completed")


if __name__ == "__main__":
    main()
