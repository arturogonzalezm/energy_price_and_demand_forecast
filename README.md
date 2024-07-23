[![codecov](https://codecov.io/gh/arturogonzalezm/energy_price_and_demand_forecast/graph/badge.svg?token=0ofxfjysER)](https://codecov.io/gh/arturogonzalezm/energy_price_and_demand_forecast)
[![PyLint](https://github.com/arturogonzalezm/energy_price_and_demand_forecast/actions/workflows/workflow.yml/badge.svg)](https://github.com/arturogonzalezm/energy_price_and_demand_forecast/actions/workflows/workflow.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](https://opensource.org/licenses/MIT)

# Energy Price and Demand Forecast

AEMO Aggregated price and demand data

# Specs

- Python 3.12
- PySpark 3.5.1
- Tested on Linux and MacOS
- PyTest (TDD)
- Codecov (Code Coverage)


# Data Processing Project

This project implements a data processing pipeline for staging, curated, and analytical layers using PySpark. It includes a thread-safe singleton logger, a SparkSession manager, and abstract and concrete data processor classes.

## Project Structure

The project consists of the following main components:

1. `SingletonLogger`: A thread-safe singleton class for logging.
2. `SparkSessionManager`: A singleton class for managing the SparkSession.
3. `DataProcessor`: An abstract base class for data processors.
4. `StagingDataProcessor`, `CuratedDataProcessor`, `AnalyticalDataProcessor`: Concrete implementations of the `DataProcessor` class for different data layers.
5. `main()` function: Orchestrates the data processing pipeline.

## Class Diagram

```mermaid
classDiagram
    class SingletonLogger {
        -_instance: SingletonLogger
        -_lock: threading.RLock
        -_logger: logging.Logger
        +__new__(logger_name, log_level, log_format)
        -_initialize_logger(logger_name, log_level, log_format)
        -_update_logger(log_level, log_format)
        -_create_handler()
        +get_logger()
    }

    class SparkSessionManager {
        -_instance: SparkSession
        +get_instance()
        +stop_instance()
    }

    class DataProcessor {
        <<abstract>>
        +spark: SparkSession
        +logger: logging.Logger
        +process_data(region, year)
        +get_input_files(region, year)*
        +read_data(input_file)*
        +clean_data(df)*
        +transform_data(df)*
        +feature_engineering(df)*
        +write_data(df, region, year, month)*
        +extract_month(file_name)*
    }

    class StagingDataProcessor {
        +get_input_files(region, year)
        +read_data(input_file)
        +clean_data(df)
        +transform_data(df)
        +feature_engineering(df)
        +write_data(df, region, year, month)
        +extract_month(file_name)
    }

    class CuratedDataProcessor {
        +get_input_files(region, year)
        +read_data(input_file)
        +clean_data(df)
        +transform_data(df)
        +feature_engineering(df)
        +write_data(df, region, year, month)
        +extract_month(file_name)
    }

    class AnalyticalDataProcessor {
        +get_input_files(region, year)
        +read_data(input_file)
        +clean_data(df)
        +transform_data(df)
        +feature_engineering(df)
        +write_data(df, region, year, month)
        +extract_month(file_name)
    }

    DataProcessor <|-- StagingDataProcessor
    DataProcessor <|-- CuratedDataProcessor
    DataProcessor <|-- AnalyticalDataProcessor
```

## Sequence Diagram

```mermaid
sequenceDiagram
    participant Main
    participant SparkSessionManager
    participant SingletonLogger
    participant StagingDataProcessor
    participant CuratedDataProcessor
    participant AnalyticalDataProcessor

    Main->>SparkSessionManager: get_instance()
    Main->>SingletonLogger: get_logger()
    Main->>StagingDataProcessor: create(spark, logger)
    Main->>CuratedDataProcessor: create(spark, logger)
    Main->>AnalyticalDataProcessor: create(spark, logger)
    
    loop For each processor
        Main->>StagingDataProcessor: process_data(region, year)
        StagingDataProcessor->>StagingDataProcessor: get_input_files(region, year)
        loop For each input file
            StagingDataProcessor->>StagingDataProcessor: read_data(input_file)
            StagingDataProcessor->>StagingDataProcessor: clean_data(df)
            StagingDataProcessor->>StagingDataProcessor: transform_data(df)
            StagingDataProcessor->>StagingDataProcessor: feature_engineering(df)
            StagingDataProcessor->>StagingDataProcessor: write_data(df, region, year, month)
        end
        Main->>CuratedDataProcessor: process_data(region, year)
        Main->>AnalyticalDataProcessor: process_data(region, year)
    end
```

## Flowchart

```mermaid
graph TD
    A[Start] --> B[Initialize SparkSession]
    B --> C[Initialize Logger]
    C --> D[Create Data Processors]
    D --> E{Process Staging Data}
    E --> F{Process Curated Data}
    F --> G{Process Analytical Data}
    G --> H[Stop SparkSession]
    H --> I[End]

    E --> J[Get Input Files]
    J --> K[Read Data]
    K --> L[Clean Data]
    L --> M[Transform Data]
    M --> N[Feature Engineering]
    N --> O[Write Data]
    O --> E

    F --> P[Get Input Files]
    P --> Q[Read Data]
    Q --> R[Clean Data]
    R --> S[Transform Data]
    S --> T[Feature Engineering]
    T --> U[Write Data]
    U --> F

    G --> V[Get Input Files]
    V --> W[Read Data]
    W --> X[Clean Data]
    X --> Y[Transform Data]
    Y --> Z[Feature Engineering]
    Z --> AA[Write Data]
    AA --> G
```

## Usage

To run the data processing pipeline, execute the `main()` function in the main module. This will process data for all specified regions and years through the staging, curated, and analytical layers.

The `SingletonLogger` and `SparkSessionManager` classes ensure that only one instance of the logger and SparkSession are created and used throughout the application, promoting consistency and resource efficiency.

Each data processor (`StagingDataProcessor`, `CuratedDataProcessor`, and `AnalyticalDataProcessor`) implements the abstract `DataProcessor` class, providing specific implementations for reading, cleaning, transforming, and writing data for their respective layers.

The main function orchestrates the entire process, creating instances of each processor and running them for each specified region and year.

# Testing

```bash
python main.py
```

# Design Patterns in the Energy Price and Demand Forecast Data Project

This project implements several design patterns to promote code reusability, flexibility, and maintainability. Let's examine each of these patterns:

## 1. Singleton Pattern

The Singleton pattern is used twice in this project:

### a. SingletonLogger

```python
class SingletonLogger:
    _instance = None
    _lock = threading.RLock()

    def __new__(cls, logger_name=None, log_level=logging.DEBUG, log_format=None):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize_logger(logger_name, log_level, log_format)
        else:
            cls._instance._update_logger(log_level, log_format)
        return cls._instance
```

This implementation ensures that only one instance of the logger is created and used throughout the application. It uses double-checked locking for thread safety.

### b. SparkSessionManager

```python
class SparkSessionManager:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = SparkSession.builder.appName("DefaultApp").getOrCreate()
        return cls._instance
```

This class ensures that only one SparkSession is created and reused across the application.

The Singleton pattern is used here to:
- Ensure consistent logging across the application
- Manage resources efficiently by reusing the SparkSession

## 2. Template Method Pattern

The Template Method pattern is implemented in the `DataProcessor` abstract base class:

```python
class DataProcessor(ABC):
    def process_data(self, region, year):
        try:
            input_files = self.get_input_files(region, year)
            for input_file in input_files:
                df = self.read_data(input_file)
                df_cleaned = self.clean_data(df)
                df_transformed = self.transform_data(df_cleaned)
                df_features = self.feature_engineering(df_transformed)
                month = self.extract_month(input_file)
                self.write_data(df_features, region, year, month)
        except Exception as e:
            self.logger.error("Error in process_data for %s in %s: %s", region, year, str(e))
```

The `process_data` method defines the skeleton of the algorithm, with abstract methods that are implemented by subclasses:

```python
@abstractmethod
def get_input_files(self, region, year):
    pass

@abstractmethod
def read_data(self, input_file):
    pass

# ... other abstract methods
```

This pattern allows the subclasses (`StagingDataProcessor`, `CuratedDataProcessor`, and `AnalyticalDataProcessor`) to provide specific implementations for each step of the data processing while keeping the overall algorithm structure consistent.

## 3. Factory Method Pattern

While not explicitly implemented as a separate factory class, the `DataProcessor` abstract base class acts as a creator in the Factory Method pattern. Each concrete subclass (`StagingDataProcessor`, `CuratedDataProcessor`, `AnalyticalDataProcessor`) creates specific data processing objects:

```python
class StagingDataProcessor(DataProcessor):
    def read_data(self, input_file):
        schema = StructType([
            StructField("REGION", StringType(), True),
            StructField("SETTLEMENTDATE", StringType(), True),
            StructField("TOTALDEMAND", DoubleType(), True),
            StructField("RRP", DoubleType(), True),
            StructField("PERIODTYPE", StringType(), True),
        ])
        return self.spark.read.csv(input_file, header=True, schema=schema)

class CuratedDataProcessor(DataProcessor):
    def read_data(self, input_file):
        return self.spark.read.parquet(input_file)

class AnalyticalDataProcessor(DataProcessor):
    def read_data(self, input_file):
        return self.spark.read.parquet(input_file)
```

Each subclass "factory" creates data frames with specific schemas and from different data sources.

## 4. Strategy Pattern

The Strategy pattern is implicitly used in the way different data processors are employed in the main function:

```python
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
```

Here, each processor represents a different strategy for data processing. The main function can work with any processor that follows the `DataProcessor` interface, allowing for flexible and interchangeable data processing strategies.

## Benefits of These Patterns

1. **Singleton**: Ensures consistent logging and efficient resource management.
2. **Template Method**: Provides a consistent structure for data processing while allowing for customization in specific steps.
3. **Factory Method**: Allows for creation of specific data processing objects without tightly coupling the main code to specific classes.
4. **Strategy**: Enables easy swapping of data processing algorithms and addition of new processors without changing the main code.

These patterns work together to create a flexible, maintainable, and extensible data processing system.


