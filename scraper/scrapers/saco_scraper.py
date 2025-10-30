import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException, InvalidSessionIdException, NoSuchElementException
from bs4 import BeautifulSoup
from urllib.parse import quote
from utils import parse_volume_string, parse_count_string, parse_saco_count_string
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from log_config import get_logger

logger = get_logger()

class SacoScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent
        self.base_url = "https://www.saco.sa/en/"

    def _log(self, msg):
        logger.info(msg)

    def _handle_overlays(self, driver):
        try:
            cookie_accept_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept')]"))
            )
            logger.info("    > Cookie banner detected. Clicking 'Accept'.")
            driver.execute_script("arguments[0].click();", cookie_accept_button)
            time.sleep(2)
        except TimeoutException:
            logger.debug("    > No cookie banner detected. Continuing.")
            pass

    def _extract_product_details(self, driver, product_url, search_mode):
        logger.debug(f"        -> Extracting details from: {product_url}")
        driver.get(product_url)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.product-title"))
        )
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        details = {
            'Product': 'Not found', 'Price_SAR': '0.00', 'Company': 'Brand not found',
            'URL': product_url, 'Unit of measurement': 'units', 'Total quantity': 0
        }

        title_tag = soup.select_one("h1.product-title")
        product_name = title_tag.get_text(strip=True) if title_tag else "Not found"
        details['Product'] = product_name

        price_tag = soup.select_one("span.discount-price")
        if price_tag:
            price_text = price_tag.get_text(separator='.', strip=True)
            details['Price_SAR'] = price_text

        additional_info_items = soup.select("ul.details-box li")
        for item in additional_info_items:
            label_tag = item.find("label")
            if label_tag and "Brand:" in label_tag.get_text():
                brand_span = label_tag.find_next_sibling("span")
                if brand_span:
                    details['Company'] = brand_span.get_text(strip=True)
                    break

        if product_name != "Not found":
            if search_mode == 'units':
                parsed_data = parse_saco_count_string(product_name)
                if not parsed_data:
                    parsed_data = parse_count_string(product_name)
            else:
                parsed_data = parse_volume_string(product_name)

            if parsed_data:
                details['Total quantity'] = parsed_data['quantity']
                details['Unit of measurement'] = parsed_data['unit']
                logger.debug(f"        -> Extracted amount: {details['Total quantity']} {details['Unit of measurement']}")

        return details

    def scrape(self, keyword, search_mode):
        self._log(f"  [Saco Scraper] Searching: '{keyword}'")
        search_keyword = quote(keyword)
        search_url = f"{self.base_url}search/{search_keyword}"
        
        all_found_products = []
        page_num = 1
        products_to_find_limit = 1

        service = ChromeService(executable_path=self.driver_path)
        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-notifications')
        # options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-webgl')
        options.add_argument('--disable-3d-apis')
        options.add_argument(f"user-agent={config.USER_AGENT}")
        options.add_argument('--log-level=3')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions ')
        options.add_argument('--disable-browser-side-navigation')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        driver = webdriver.Chrome(service=service, options=options)

        driver.get(search_url)
        self._handle_overlays(driver)

        try:
            WebDriverWait(driver, 25).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-inner-container"))
            )
        except TimeoutException:
            logger.warning("    > No product containers found on the initial page. Skipping keyword.")
            if driver:
                driver.quit()
            return []

        while len(all_found_products) < products_to_find_limit:
            logger.info(f"--- Analyzing Page {page_num} ---")
            
            search_page_url = driver.current_url
            time.sleep(5)

            
            product_containers = driver.find_elements(By.CSS_SELECTOR, "div.product-inner-container")
            num_containers = len(product_containers)

            if num_containers == 0:
                logger.warning("    ! No products containers found on this page.")
                break

            logger.debug(f"    > Found {num_containers} product containers on this page.")

            
            for i in range(num_containers):
                if len(all_found_products) >= products_to_find_limit:
                    break
                try:
                    
                    current_containers = driver.find_elements(By.CSS_SELECTOR, "div.product-inner-container")
                    if i >= len(current_containers):
                        break
                    
                    container = current_containers[i]

                    
                    try:
                        product_link = container.find_element(By.CSS_SELECTOR, "p.product-name a")
                        if not product_link.text.strip():
                            logger.debug(f"      -> Skipping empty product container #{i+1}.")
                            continue 
                    except NoSuchElementException:
                        logger.debug(f"      -> Skipping container #{i+1} (no product link found inside).")
                        continue 

                    logger.debug(f"      -> Processing product {i+1}/{num_containers}...")
                    
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", product_link)
                    time.sleep(1)
                    product_link.click()

                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "h1.product-title"))
                    )
                    
                    product_url = driver.current_url
                    product_details = self._extract_product_details(driver, product_url, search_mode)
                    
                    if product_details and product_details.get('Total quantity', 0) > 0:
                        is_relevant = self.relevance_agent.is_relevant(product_details.get('Product'), keyword)
                        if is_relevant:
                            all_found_products.append(product_details)
                            logger.info(f"      -> AI VALIDATED. Product saved: {product_details['Product'][:60]}...")
                        else:
                            logger.info(f"      -> DISCARDED BY AI (Not relevant): {product_details['Product'][:60]}...")
                    else:
                        logger.info(f"      -> DISCARDED (no quantity): {product_details.get('Product', 'N/A')[:60]}...")

                    driver.get(search_page_url)
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-inner-container"))
                    )
                    time.sleep(2)

                except (TimeoutException, StaleElementReferenceException, ElementClickInterceptedException) as e:
                    logger.warning(f"      -> WARNING: Could not process product {i+1}. Skipping. Reason: {type(e).__name__}")
                    driver.get(search_page_url)
                    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-inner-container")))
                    continue
                except InvalidSessionIdException:
                    logger.error(f"      -> FATAL ERROR: Browser session lost. Aborting scrape for '{keyword}'.")
                    if driver:
                        driver.quit()
                    return all_found_products
            
            if len(all_found_products) >= products_to_find_limit:
                logger.info(f"    > Target of {products_to_find_limit} products reached.")
                break
            
            try:
                current_page_url = driver.current_url
                next_page_button = driver.find_element(By.CSS_SELECTOR, "a.next")
                logger.debug("    > Next page button found. Attempting to navigate...")
                
                driver.execute_script("arguments[0].click();", next_page_button)
                time.sleep(3)

                if driver.current_url == current_page_url:
                    logger.info("    > URL did not change. Reached the last page.")
                    break
                else:
                    page_num += 1
                    logger.info("    > Successfully navigated to the next page.")

            except:
                logger.debug("    > No more pages found. Ending pagination.")
                break

        logger.info(f"\n  [Saco Scraper] Finished scraping. Found data for {len(all_found_products)} products.")
        if driver:
            driver.quit()
        return all_found_products