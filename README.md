[![codecov](https://codecov.io/gh/arturogonzalezm/energy_price_and_demand_forecast/graph/badge.svg?token=0ofxfjysER)](https://codecov.io/gh/arturogonzalezm/energy_price_and_demand_forecast)
[![PyLint](https://github.com/arturogonzalezm/energy_price_and_demand_forecast/actions/workflows/workflow.yml/badge.svg)](https://github.com/arturogonzalezm/energy_price_and_demand_forecast/actions/workflows/workflow.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](https://opensource.org/licenses/MIT)

# Energy Price and Demand Forecast

AEMO Aggregated price and demand data

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

