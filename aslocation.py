import json
import re
import time
import os
import pandas as pd
import random
import logging
import urllib.parse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# === CONFIGURATION ===
MIN_DELAY = 5
MAX_DELAY = 8
CHECKPOINT_FILE = "emails_result.json"
CSV_OUTPUT_FILE = "emails_output.csv"
LOCATIONS_COLUMN = "Location"  # Excel column name for USA locations (adjust as needed)
MAX_RETRIES = 3

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log", encoding="utf-8")  # Use UTF-8 for log file
    ]
)

def load_locations_from_excel(file_path, column_name=LOCATIONS_COLUMN):
    """
    Load USA locations from the specified column of the Excel file.
    """
    try:
        df = pd.read_excel(file_path)
        if column_name not in df.columns:
            logging.error(f"Column '{column_name}' not found in Excel file: {file_path}")
            return []
        locations = df[column_name].dropna().astype(str).str.strip()
        seen = set()
        unique_locations = [loc for loc in locations if loc and not (loc in seen or seen.add(loc))]
        if not unique_locations:
            logging.error(f"No valid USA locations found in column '{column_name}' of {file_path}")
            return []
        logging.info(f"Loaded {len(unique_locations)} unique USA locations from {file_path}")
        return unique_locations
    except FileNotFoundError:
        logging.error(f"Excel file not found at: {file_path}")
        return []
    except Exception as e:
        logging.error(f"Error loading Excel file {file_path}: {str(e)}")
        return []

def load_checkpoint(checkpoint_file=CHECKPOINT_FILE):
    """
    Load existing results from the checkpoint file to resume scraping.
    """
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    logging.info(f"Checkpoint file {checkpoint_file} is empty. Starting fresh.")
                    return {}
                return json.loads(content)
        except json.JSONDecodeError as e:
            logging.error(f"Corrupted checkpoint file {checkpoint_file}: {e}. Starting fresh.")
            return {}
        except Exception as e:
            logging.error(f"Error loading checkpoint file {checkpoint_file}: {e}")
            return {}
    return {}

def save_checkpoint(email_results, checkpoint_file=CHECKPOINT_FILE):
    """
    Save current results to the checkpoint file.
    """
    try:
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(email_results, f, indent=4)
        logging.info(f"Checkpoint saved to: {checkpoint_file}")
    except Exception as e:
        logging.error(f"Error saving checkpoint to {checkpoint_file}: {str(e)}")

def export_to_csv(email_results, csv_file=CSV_OUTPUT_FILE):
    """
    Export email results to a CSV file.
    """
    try:
        data = [{"Location": loc, "Email": email} for loc, emails in email_results.items() for email in emails]
        df = pd.DataFrame(data)
        df.to_csv(csv_file, index=False, encoding="utf-8")
        logging.info(f"Results exported to: {csv_file}")
    except Exception as e:
        logging.error(f"Error exporting to CSV {csv_file}: {str(e)}")

def initialize_driver(extension_path, extension_path2):
    """
    Initialize the Chrome WebDriver with specified extensions and anti-detection options.
    """
    for path in [extension_path, extension_path2]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Extension file not found at: {path}")

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # Run in headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_extension(extension_path)
    chrome_options.add_extension(extension_path2)
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
    ]
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })
        return driver
    except Exception as e:
        logging.error(f"Error initializing WebDriver: {str(e)}")
        return None

def scrape_google_emails(extension_path, extension_path2, locations):
    """
    Scrape Google search results for emails using the provided USA locations.
    Query: from:construction inurl:contact {location} email
    """
    email_results = load_checkpoint()
    driver = None

    try:
        driver = initialize_driver(extension_path, extension_path2)
        if not driver:
            raise Exception("Failed to initialize WebDriver")

        for location in locations:
            if location in email_results:
                logging.info(f"Skipping already processed location: {location}")
                continue

            query = f'from:construction inurl:contact "{location}" email'
            logging.info(f"Searching: {query}")
            encoded_query = urllib.parse.quote(query)
            url = f"https://www.google.com/search?q={encoded_query}&num=50"

            for attempt in range(MAX_RETRIES):
                try:
                    driver.get(url)
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div#search"))
                    )
                    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

                    # Check for CAPTCHA
                    if "Our systems have detected unusual traffic" in driver.page_source:
                        logging.warning(f"CAPTCHA detected for {location}. Relying on Buster extension.")
                        time.sleep(10)  # Give Buster time to solve

                    # Scroll to load all results
                    last_height = driver.execute_script("return document.body.scrollHeight")
                    while True:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(random.uniform(2, 4))
                        new_height = driver.execute_script("return document.body.scrollHeight")
                        if new_height == last_height:
                            break
                        last_height = new_height

                    page_html = driver.page_source
                    # Improved email regex
                    emails = set(re.findall(
                        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?!\.(png|jpg|jpeg|gif|bmp))',
                        page_html,
                        re.IGNORECASE
                    ))
                    email_results[location] = sorted(list(emails))

                    logging.info(f"Found {len(emails)} email(s) for: {location}")
                    for email in emails:
                        logging.info(email)

                    save_checkpoint(email_results)
                    export_to_csv(email_results)
                    break  # Success, move to next location

                except TimeoutException:
                    logging.error(f"Timeout loading search results for {location}. Attempt {attempt + 1}/{MAX_RETRIES}")
                    if attempt == MAX_RETRIES - 1:
                        logging.error(f"Max retries reached for {location}. Skipping.")
                        email_results[location] = []
                        save_checkpoint(email_results)
                        export_to_csv(email_results)
                    time.sleep(random.uniform(5, 10))
                    continue
                except WebDriverException as e:
                    logging.error(f"WebDriver error for {location}: {e}")
                    if driver:
                        driver.quit()
                    driver = initialize_driver(extension_path, extension_path2)
                    if not driver:
                        logging.error(f"Failed to restart WebDriver. Saving results and exiting.")
                        save_checkpoint(email_results)
                        export_to_csv(email_results)
                        return email_results
                    logging.info(f"WebDriver restarted. Retrying {location}.")
                    continue

    except Exception as e:
        logging.error(f"Error during scraping: {str(e)}")
        save_checkpoint(email_results)
        export_to_csv(email_results)

    finally:
        if driver:
            driver.quit()
            logging.info("Browser closed.")

    return email_results

if __name__ == "__main__":
    # Relative paths for portability
    base_dir = os.path.dirname(__file__)
    extension_path = os.path.join(base_dir, "extensions", "KDPLAPECIAGKKJOIGNNKFPBFKEBCFBPB_0_3_24_0.crx")
    extension_path2 = os.path.join(base_dir, "extensions", "Buster.crx")
    excel_file_path = os.path.join(base_dir, "data", "country.xlsx")

    try:
        for path in [excel_file_path, extension_path, extension_path2]:
            if not os.path.exists(path):
                if path == excel_file_path:
                    logging.error(
                        f"Excel file not found at: {path}. "
                        "Please ensure 'country.xlsx' exists in the 'data' folder with a column named '{LOCATIONS_COLUMN}'."
                    )
                else:
                    logging.error(f"Extension file not found at: {path}")
                raise FileNotFoundError(f"Missing: {path}")

        locations = load_locations_from_excel(excel_file_path)
        if not locations:
            logging.error("No USA locations loaded. Please check the Excel file and column name.")
            exit(1)

        all_emails = scrape_google_emails(extension_path, extension_path2, locations)
        total = sum(len(emails) for emails in all_emails.values())
        logging.info(f"All scraping done. Total unique emails found: {total}")

    except FileNotFoundError as e:
        logging.error(f"Script execution failed: {str(e)}")
        exit(1)
    except Exception as e:
        logging.error(f"Script execution failed: {str(e)}")
        exit(1)