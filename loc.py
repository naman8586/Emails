import pandas as pd
import time
import re
import csv
import json
import random
import logging
import urllib.parse
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# === CONFIGURATION ===
MIN_DELAY = 5
MAX_DELAY = 8
CHECKPOINT_FILE = "emails_result.json"
CSV_OUTPUT_FILE = "scraped_emails.csv"
MAX_RETRIES = 3
MAX_LINKS_PER_PAGE = 5  # Limit links to visit per search page
PAGE_LOAD_TIMEOUT = 15  # Timeout for page loading
DYNAMIC_WAIT = 3  # Wait for dynamic content

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

def load_cities_and_keywords(excel_path, sheet_name="Sheet1"):
    """Load USA and UK cities and keywords from the Excel file."""
    try:
        df_cities = pd.read_excel(excel_path, sheet_name=sheet_name)
        usa_cities = df_cities['USA'].dropna().astype(str).str.strip().tolist()
        uk_cities = df_cities['UK'].dropna().astype(str).str.strip().tolist()
        
        df_keywords = pd.read_excel(excel_path, sheet_name='Sheet2')
        keywords = df_keywords['Keywords'].dropna().astype(str).str.strip().tolist()
        
        # Remove duplicates
        usa_cities = list(dict.fromkeys(usa_cities))
        uk_cities = list(dict.fromkeys(uk_cities))
        keywords = list(dict.fromkeys(keywords))
        
        logger.info(f"Loaded {len(usa_cities)} USA cities, {len(uk_cities)} UK cities, {len(keywords)} keywords")
        return usa_cities, uk_cities, keywords
    except Exception as e:
        logger.error(f"Error reading Excel file {excel_path}: {e}")
        return [], [], []

def load_checkpoint(checkpoint_file=CHECKPOINT_FILE):
    """Load existing results from the checkpoint file."""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    logger.info(f"Checkpoint file {checkpoint_file} is empty. Starting fresh.")
                    return {}
                return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Corrupted checkpoint file {checkpoint_file}: {e}. Starting fresh.")
            return {}
        except Exception as e:
            logger.error(f"Error loading checkpoint file {checkpoint_file}: {e}")
            return {}
    return {}

def save_checkpoint(email_results, checkpoint_file=CHECKPOINT_FILE):
    """Save current results to the checkpoint file."""
    try:
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(email_results, f, indent=4)
        logger.info(f"Checkpoint saved to: {checkpoint_file}")
    except Exception as e:
        logger.error(f"Error saving checkpoint to {checkpoint_file}: {e}")

def export_to_csv(email_results, csv_file=CSV_OUTPUT_FILE):
    """Export email results to a CSV file."""
    try:
        data = [
            {"Keyword": keyword, "City": city, "Email": email}
            for (keyword, city), emails in email_results.items()
            for email in emails if email  # Exclude empty strings
        ]
        df = pd.DataFrame(data)
        df.to_csv(csv_file, index=False, encoding="utf-8")
        logger.info(f"Results exported to: {csv_file}")
    except Exception as e:
        logger.error(f"Error exporting to CSV {csv_file}: {e}")

def setup_driver(extension_paths):
    """Set up Chrome driver with extensions."""
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")  # Run in headless mode
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--start-maximized")
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"
        ]
        chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        for ext_path in extension_paths:
            if not os.path.exists(ext_path):
                logger.error(f"Extension file not found: {ext_path}")
                return None
            logger.info(f"Loading extension: {ext_path}")
            chrome_options.add_extension(ext_path)
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager(driver_version="136.0.7103.114").install()),
            options=chrome_options
        )
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })
        logger.info("ChromeDriver initialized successfully")
        return driver
    except Exception as e:
        logger.error(f"Error setting up driver: {e}")
        return None

def extract_emails(text):
    """Extract email addresses from text using regex."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?!\.(png|jpg|jpeg|gif|bmp|svg|webp))'
    emails = set(re.findall(email_pattern, text, re.IGNORECASE))
    # Filter out invalid emails
    valid_emails = {email for email in emails if len(email) > 5 and '@' in email and '.' in email.split('@')[1]}
    return valid_emails

def scrape_emails(driver, query):
    """Scrape emails from Google search results and contact pages."""
    emails = set()
    try:
        encoded_query = urllib.parse.quote(query)
        search_url = f"https://www.google.com/search?q={encoded_query}&num=50"
        driver.get(search_url)
        WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#search"))
        )
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        # Check for CAPTCHA
        if "Our systems have detected unusual traffic" in driver.page_source:
            logger.warning(f"CAPTCHA detected for query: {query}. Relying on Buster extension.")
            time.sleep(10)  # Give Buster time to solve
        
        # Extract emails from search results page
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        page_text = soup.get_text(separator=' ', strip=True)
        page_emails = extract_emails(page_text)
        emails.update(page_emails)
        logger.info(f"Extracted {len(page_emails)} emails from search results page for query: {query}")
        
        # Get search result links
        results = WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.yuRUbf a'))
        )
        links = [result.get_attribute('href') for result in results if result.get_attribute('href') and 'http' in result.get_attribute('href')]
        logger.info(f"Found {len(links)} valid links for query: {query}")
        
        # Visit each link to scrape emails
        original_window = driver.current_window_handle
        for link in links[:MAX_LINKS_PER_PAGE]:
            try:
                driver.execute_script(f"window.open('{link}');")
                driver.switch_to.window(driver.window_handles[-1])
                WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )
                time.sleep(DYNAMIC_WAIT)  # Wait for dynamic content
                
                # Parse page with BeautifulSoup
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                page_text = soup.get_text(separator=' ', strip=True)
                page_emails = extract_emails(page_text)
                emails.update(page_emails)
                logger.info(f"Extracted {len(page_emails)} emails from {link}")
                
                driver.close()
                driver.switch_to.window(original_window)
            except Exception as e:
                logger.error(f"Error scraping link {link}: {e}")
                try:
                    driver.close()
                except:
                    pass
                driver.switch_to.window(original_window)
                continue
        
        logger.info(f"Total emails found for query '{query}': {len(emails)}")
        return emails
    except TimeoutException:
        logger.error(f"Timeout loading search results for query: {query}")
        return set()
    except Exception as e:
        logger.error(f"Error scraping emails for query '{query}': {e}")
        return set()

def main():
    # Configuration
    excel_path = r"D:\Emails\data\country.xlsx"
    extension_paths = [
        r"D:\Emails\extensions\KDPLAPECIAGKKJOIGNNKFPBFKEBCFBPB_0_3_24_0.crx",
        r"D:\Emails\extensions\Buster.crx"
    ]
    
    # Verify file paths
    for path in [excel_path] + extension_paths:
        if not os.path.exists(path):
            logger.error(f"File not found: {path}")
            return
    
    # Load cities and keywords
    usa_cities, uk_cities, keywords = load_cities_and_keywords(excel_path)
    all_cities = usa_cities + uk_cities
    
    if not all_cities or not keywords:
        logger.error("No cities or keywords loaded. Exiting.")
        return
    
    # Load checkpoint
    email_results = load_checkpoint()
    
    # Set up driver
    driver = setup_driver(extension_paths)
    if not driver:
        logger.error("Driver setup failed. Exiting.")
        return
    
    try:
        # Limit for testing
        for keyword in keywords[:1]:  # Test with 1 keyword
            for city in all_cities[:5]:  # Test with 5 cities
                key = (keyword, city)
                if str(key) in email_results:
                    logger.info(f"Skipping already processed: {keyword} in {city}")
                    continue
                
                logger.info(f"Scraping emails for keyword: {keyword}, city: {city}")
                query = f'{keyword} inurl:contact "{city}"'
                emails = set()
                for attempt in range(MAX_RETRIES):
                    try:
                        emails = scrape_emails(driver, query)
                        break
                    except WebDriverException:
                        logger.error(f"WebDriver error for {keyword} in {city}. Attempt {attempt + 1}/{MAX_RETRIES}")
                        driver.quit()
                        driver = setup_driver(extension_paths)
                        if not driver:
                            logger.error("Failed to restart WebDriver. Saving and exiting.")
                            save_checkpoint(email_results)
                            export_to_csv(email_results)
                            return
                        logger.info("WebDriver restarted.")
                
                email_results[str(key)] = sorted(list(emails))
                save_checkpoint(email_results)
                export_to_csv(email_results)
                
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        logger.info(f"Scraping complete. Total emails found: {sum(len(emails) for emails in email_results.values())}")
    
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    
    finally:
        driver.quit()
        logger.info("Browser closed.")

if __name__ == "__main__":
    main()