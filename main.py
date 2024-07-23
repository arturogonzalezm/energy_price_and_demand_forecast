from src.layers.staging_layer import process_staging_data
from src.layers.curated_layer import process_curated_data
from src.layers.analytical_layer import process_analytical_data
from src.layers.ml_forecasting_layer import process_ml_forecasting_data


def main():
    regions = ["NSW", "VIC", "QLD", "TAS", "SA"]
    year = "2020"

    for region in regions:
        # TODO: Process one at a time sequentially
        # Process staging data
        process_staging_data(region, year)

        # # Process curated data
        process_curated_data(region, year)
        #
        # # Process analytical data
        process_analytical_data(region, year)

        # Process ML and forecasting data
        process_ml_forecasting_data(region, year)


if __name__ == '__main__':
    main()
