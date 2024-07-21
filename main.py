from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib3.exceptions import MaxRetryError
from requests.exceptions import ConnectionError
import time
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    download_dir = os.path.abspath("data/raw")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
    })

    chrome_options.page_load_strategy = "normal"

    service = ChromeService(ChromeDriverManager().install())

    for attempt in range(3):  # Try 3 times
        try:
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(120)  # Increase timeout to 120 seconds
            return driver
        except (ConnectionError, MaxRetryError) as e:
            if attempt < 2:  # If not the last attempt
                logging.warning(f"Connection error, retrying... (Attempt {attempt + 1})")
                time.sleep(5)  # Wait 5 seconds before retrying
            else:
                raise  # If all attempts fail, raise the exception

    raise Exception("Failed to create WebDriver after multiple attempts")


def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)


def wait_for_download(directory, timeout=60):
    end_time = time.time() + timeout
    while time.time() < end_time:
        files = [f for f in os.listdir(directory) if f.endswith('.csv') and not f.startswith('.')]
        if files:
            return files[0]  # Return the name of the first CSV file found
        time.sleep(1)
    return None


def move_latest_download(source_dir, destination_dir, region, year, month):
    downloaded_file = wait_for_download(source_dir)
    if downloaded_file:
        source_file = os.path.join(source_dir, downloaded_file)
        new_path = os.path.join(destination_dir, downloaded_file)
        os.rename(source_file, new_path)
        logging.info(f"Moved {source_file} to {new_path}")
    else:
        logging.warning(f"No CSV file found to move for region {region}, year {year}, month {month}")


def wait_for_page_load(driver, timeout=120):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, "root"))
        )
    except TimeoutException:
        logging.warning("Timeout waiting for page to load completely. Proceeding anyway.")


def wait_for_element(driver, by, value, timeout=30):
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        return element
    except TimeoutException:
        logging.error(f"Timeout waiting for element: {value}")
        return None


def safe_select(select_element, value):
    try:
        Select(select_element).select_by_value(value)
        return True
    except Exception as e:
        logging.warning(f"Failed to select value {value}: {str(e)}")
        return False


def check_download_button(driver):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.visualisation-button"))
        )
        return True
    except TimeoutException:
        logging.warning("Download button not found")
        return False


def download_data(driver, data_type="historical"):
    url = "https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data"
    max_retries = 3
    for attempt in range(max_retries):
        try:
            driver.get(url)
            wait_for_page_load(driver)
            logging.info(f"Navigated to {url}")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Failed to load page (attempt {attempt + 1}): {str(e)}. Retrying...")
                time.sleep(5)
            else:
                logging.error(f"Failed to load page after {max_retries} attempts: {str(e)}")
                return

    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

    for region in regions:
        try:
            # Check if the element is inside an iframe
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            for iframe in iframes:
                driver.switch_to.frame(iframe)
                region_select = wait_for_element(driver, By.ID, "region-select")
                if region_select:
                    break
                driver.switch_to.default_content()

            if not region_select:
                logging.error("Region select element not found")
                continue

            if not safe_select(region_select, region):
                logging.error(f"Failed to select region: {region}")
                continue

            logging.info(f"Selected region: {region}")

            if data_type == "historical":
                year_select = wait_for_element(driver, By.ID, "year-select")
                if not year_select:
                    logging.error("Year select element not found")
                    continue

                years = [option.get_attribute("value") for option in Select(year_select).options]

                for year in years:
                    if not safe_select(year_select, year):
                        logging.error(f"Failed to select year: {year}")
                        continue

                    logging.info(f"Selected year: {year}")

                    month_select = wait_for_element(driver, By.ID, "month-select")
                    if not month_select:
                        logging.error("Month select element not found")
                        continue

                    months = [option.get_attribute("value") for option in Select(month_select).options]

                    for month in months:
                        if not safe_select(month_select, month):
                            logging.error(f"Failed to select month: {month}")
                            continue

                        logging.info(f"Selected month: {month}")

                        if not check_download_button(driver):
                            logging.error("Download button not found. Skipping download.")
                            continue

                        download_button = driver.find_element(By.CSS_SELECTOR, "a.visualisation-button")
                        download_button.click()
                        logging.info("Clicked download button for historical data")

                        time.sleep(5)  # Wait for download to start

                        destination_dir = os.path.abspath(f"data/raw/{region}/historical/{year}")
                        create_directory(destination_dir)
                        move_latest_download(os.path.abspath("data/raw"), destination_dir, region, year, month)
            else:
                if not check_download_button(driver):
                    logging.error("Download button not found. Skipping download.")
                    continue

                current_month_button = driver.find_element(By.CSS_SELECTOR, "a.visualisation-button")
                current_month_button.click()
                logging.info("Clicked download button for current month data")

                time.sleep(5)  # Wait for download to start

                destination_dir = os.path.abspath(f"data/raw/{region}/current")
                create_directory(destination_dir)
                move_latest_download(os.path.abspath("data/raw"), destination_dir, region,
                                     datetime.now().strftime("%Y"), datetime.now().strftime("%m"))
        except Exception as e:
            logging.error(f"Error processing region {region}: {str(e)}")


def main():
    driver = None
    try:
        driver = setup_driver()
        download_data(driver, "historical")
        # download_data(driver, "current")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}", exc_info=True)
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    main()
