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
from selenium.common.exceptions import WebDriverException
# === CONFIGURATION ===
MIN_DELAY = 5
MAX_DELAY = 8
REAL_ESTATE_CHECKPOINT = "real_estate_emails.json"
CONSTRUCTION_CHECKPOINT = "construction_emails.json"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("scraper.log")]
)

def load_locations_from_excel(file_path):
    """
    Load locations from the Excel file:
    - USA locations from column 4.
    - UK locations from column 7.
    Returns a list of location lists: [usa_locations, uk_locations]
    """
    try:
        # Read the Excel file
        df = pd.read_excel(file_path)

        # Extract USA locations (column 4, 0-based index 3)
        usa_locations = df.iloc[:, 3].dropna().str.strip()
        usa_locations = [loc for loc in usa_locations if loc]
        seen = set()
        usa_locations = [loc for loc in usa_locations if not (loc in seen or seen.add(loc))]

        # Extract UK locations (column 7, 0-based index 6)
        uk_locations = df.iloc[:, 6].dropna().str.strip()
        uk_locations = [loc for loc in uk_locations if loc]
        seen = set()
        uk_locations = [loc for loc in uk_locations if not (loc in seen or seen.add(loc))]

        # Log counts
        logging.info(f"📍 Loaded {len(usa_locations)} unique USA locations from {file_path}")
        logging.info(f"📍 Loaded {len(uk_locations)} unique UK locations from {file_path}")

        # Check if any locations are loaded
        if not (usa_locations or uk_locations):
            logging.error("❌ No valid locations found in USA or UK columns.")
            return []

        return [usa_locations, uk_locations]
    except Exception as e:
        logging.error(f"❌ Error loading Excel file: {str(e)}")
        return []

def load_checkpoint(checkpoint_file):
    """
    Load existing results from the checkpoint file to resume scraping.
    """
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r") as f:
                content = f.read().strip()
                if not content:
                    logging.info(f"⚠️ Checkpoint file {checkpoint_file} is empty. Starting fresh.")
                    return {}
                return json.loads(content)
        except json.JSONDecodeError as e:
            logging.error(f"❌ Corrupted checkpoint file {checkpoint_file}: {e}. Starting fresh.")
            return {}
        except Exception as e:
            logging.error(f"❌ Error loading checkpoint file {checkpoint_file}: {e}")
            return {}
    return {}

def save_checkpoint(email_results, checkpoint_file):
    """
    Save current results to the checkpoint file.
    """
    try:
        with open(checkpoint_file, "w") as f:
            json.dump(email_results, f, indent=4)
        logging.info(f"💾 Checkpoint saved to: {checkpoint_file}")
    except Exception as e:
        logging.error(f"❌ Error saving checkpoint to {checkpoint_file}: {e}")

def initialize_driver(extension_path, extension_path2):
    """
    Initialize the Chrome WebDriver with specified extensions.
    """
    if not os.path.exists(extension_path):
        raise FileNotFoundError(f"Extension file not found at: {extension_path}")
    if not os.path.exists(extension_path2):
        raise FileNotFoundError(f"Buster extension file not found at: {extension_path2}")

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_extension(extension_path)
    chrome_options.add_extension(extension_path2)
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Apple_WebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
    ]
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    try:
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        return driver
    except Exception as e:
        logging.error(f"❌ Error initializing WebDriver: {str(e)}")
        return None

def scrape_google_emails(extension_path, extension_path2, locations, keyword, checkpoint_file, region_name):
    """
    Scrape Google search results for emails using the provided locations and keyword.
    Uses the query: {keyword} in "{location}" inurl:/contact "email //@*.com"
    """
    email_results = load_checkpoint(checkpoint_file)
    driver = None

    try:
        driver = initialize_driver(extension_path, extension_path2)
        if not driver:
            raise Exception("Failed to initialize WebDriver")

        # Process each location
        for location in locations:
            if location in email_results:
                logging.info(f"⏭️ Skipping already processed location: {location} ({region_name})")
                continue

            # Construct the search query
            query = f'{keyword} in "{location}" inurl:/contact "email //@*.com"'
            logging.info(f"🔍 Searching: {query} ({region_name})")
            # Properly encode the query for the URL
            encoded_query = urllib.parse.quote(query)
            url = f'https://www.google.com/search?q={encoded_query}&num=50'
            logging.info(f"🌐 Constructed URL: {url} ({region_name})")
            try:
                driver.get(url)
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

                # Scroll to load more results
                last_height = driver.execute_script("return document.body.scrollHeight")
                while True:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(random.uniform(8, 12))
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                    time.sleep(random.uniform(5, 7))

                page_html = driver.page_source
                # Improved email regex to exclude image formats
                emails = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.(?!png|jpg|jpeg)[a-zA-Z]{2,}', page_html))
                email_results[location] = sorted(list(emails))

                logging.info(f"📧 Found {len(emails)} email(s) for: {location} ({region_name})")
                for email in emails:
                    logging.info(f"  {email}")

                save_checkpoint(email_results, checkpoint_file)

            except WebDriverException as e:
                logging.error(f"❌ WebDriver error for {location} ({region_name}): {e}")
                if driver:
                    driver.quit()
                driver = initialize_driver(extension_path, extension_path2)
                if not driver:
                    logging.error(f"❌ Failed to restart WebDriver. Saving current results and exiting ({region_name}).")
                    save_checkpoint(email_results, checkpoint_file)
                    return email_results
                logging.info(f"🔄 WebDriver restarted. Retrying {location} ({region_name}).")
                continue

    except Exception as e:
        logging.error(f"❌ Error during scraping ({region_name}): {e}")
        save_checkpoint(email_results, checkpoint_file)

    finally:
        if driver:
            driver.quit()
            logging.info(f"🛑 Browser closed ({region_name}).")

    return email_results

if __name__ == "__main__":
    extension_path = "D:\\Emails\\extensions\\KDPLAPECIAGKKJOIGNNKFPBFKEBCFBPB_0_3_24_0.crx"
    extension_path2 = "D:\\Emails\\extensions\\Buster.crx"
    excel_file_path = "D:\\Emails\\data\\country.xlsx"

    try:
        for path in [excel_file_path, extension_path, extension_path2]:
            if not os.path.exists(path):
                raise FileNotFoundError(f"Missing: {path}")

        # Load locations
        location_groups = load_locations_from_excel(excel_file_path)
        if not location_groups:
            logging.error("❗ No locations loaded. Exiting.")
            exit(1)

        # Define regions and their corresponding location lists
        regions = [
            ("USA", location_groups[0] if location_groups else []),
            ("UK", location_groups[1] if len(location_groups) > 1 else [])
        ]

        # Process each region
        for region_name, locations in regions:
            if not locations:
                logging.warning(f"⚠️ No locations for {region_name}. Skipping.")
                continue

            # Phase 1: Scrape for "real estate"
            logging.info(f"🚀 Starting Phase 1: Scraping for 'real estate' in {region_name}")
            real_estate_emails = scrape_google_emails(
                extension_path, extension_path2, locations, "real estate", REAL_ESTATE_CHECKPOINT, region_name
            )
            total_real_estate = sum(len(emails) for emails in real_estate_emails.values())
            logging.info(f"✅ Phase 1 completed for {region_name}. Total unique emails found for 'real estate': {total_real_estate}")

            # Phase 2: Scrape for "construction"
            logging.info(f"🚀 Starting Phase 2: Scraping for 'construction' in {region_name}")
            construction_emails = scrape_google_emails(
                extension_path, extension_path2, locations, "construction", CONSTRUCTION_CHECKPOINT, region_name
            )
            total_construction = sum(len(emails) for emails in construction_emails.values())
            logging.info(f"✅ Phase 2 completed for {region_name}. Total unique emails found for 'construction': {total_construction}")

        # Final summary
        real_estate_emails = load_checkpoint(REAL_ESTATE_CHECKPOINT)
        construction_emails = load_checkpoint(CONSTRUCTION_CHECKPOINT)
        total_real_estate = sum(len(emails) for emails in real_estate_emails.values())
        total_construction = sum(len(emails) for emails in construction_emails.values())
        logging.info(f"🎉 All scraping done. Total emails: Real Estate = {total_real_estate}, Construction = {total_construction}")

    except Exception as e:
        logging.error(f"❗ Script execution failed: {str(e)}")