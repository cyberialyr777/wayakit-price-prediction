import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
from utils import parse_volume_string
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from log_config import get_logger

logger = get_logger()

class MumzworldScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent 
        self.base_url = "https://www.mumzworld.com/sa-en/"

    def _log(self, msg):
        logger.info(msg)

    def _safe_get_text(self, element):
        return element.get_text(strip=True) if element else None

    def _parse_mumzworld_count_string(self, text_string):
        if not text_string:
            return None
        match = re.search(r'(\d+)\s*(wipes|count|sheets|sachets|pack|pcs|pieces|pc|s)\b', text_string, re.I)
        if not match:
            return None
        quantity = int(match.group(1))
        return {'quantity': quantity, 'unit': 'units', 'normalized': quantity}

    def _extract_product_details(self, driver, product_url, search_mode):
        details = {
            'Product': 'Not found', 'Price_SAR': '0.00', 'Company': 'Not found',
            'URL': product_url, 'Unit of measurement': 'units', 'Total quantity': 0
        }
        try:
            driver.get(product_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.ProductDetails_productName__lcVK_")))
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            product_name_tag = soup.find('h1', class_='ProductDetails_productName__lcVK_')
            product_name = self._safe_get_text(product_name_tag)
            if product_name:
                details['Product'] = product_name
                details['Company'] = product_name.split(' - ')[0].strip() if ' - ' in product_name else product_name.split(' ')[0].strip()

                parsed_data = self._parse_mumzworld_count_string(product_name) if search_mode == 'units' else parse_volume_string(product_name)
                if parsed_data:
                    base_quantity = parsed_data['quantity']
                    multiplier_match = re.search(r'(?:pack of|x|of)\s*(\d+)', product_name, re.IGNORECASE)
                    if multiplier_match:
                        multiplier = int(multiplier_match.group(1))
                        details['Total quantity'] = base_quantity * multiplier
                        logger.debug(f"      -> Multiplier found: {base_quantity} * {multiplier} = {details['Total quantity']}")
                    else:
                        details['Total quantity'] = base_quantity
                    details['Unit of measurement'] = parsed_data['unit']
                    logger.debug(f"      -> Extracted amount: {details['Total quantity']} {parsed_data['unit']}")

            price_tag = soup.find('span', class_='Price_integer__3ngZQ')
            if price_tag:
                details['Price_SAR'] = self._safe_get_text(price_tag).replace(',', '')
        except Exception as e:
            logger.error(f"      ! Error extracting details from {product_url}", exc_info=True)
        return details

    def scrape(self, keyword, search_mode):
        self._log(f"  [Mumzworld Scraper] Searching: '{keyword}' (Mode: {search_mode})")
        search_url = f"{self.base_url}search?q={quote(keyword)}"
        valid_products_found = []
        products_to_find = 7

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

        try:
            logger.info(f"    > Navigating to: {search_url}")
            driver.get(search_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.ProductCard_productCard__kFgss")))
            logger.info("    > Search results page loaded. Analyzing products...")

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            product_containers = soup.select("div.ProductCard_productCard__kFgss")

            if not product_containers:
                logger.warning("    ! Warning: No product containers found.")
                return []

            for container in product_containers:
                if len(valid_products_found) >= products_to_find:
                    logger.info(f"    > Limit of {products_to_find} VALID products reached.")
                    break

                link_tag = container.find('a', class_='ProductCard_productName__Dz1Yx')
                if link_tag and link_tag.has_attr('href'):
                    product_url = urljoin(self.base_url, link_tag['href'])
                    logger.debug(f"      -> Visiting: {product_url[:80]}...")
                    product_details = self._extract_product_details(driver, product_url, search_mode)

                    if product_details.get('Total quantity', 0) > 0:
                        is_relevant = self.relevance_agent.is_relevant(product_details.get('Product'), keyword)
                        
                        if is_relevant:
                            valid_products_found.append(product_details)
                            logger.info(f"      -> VALID. Extracted: {product_details['Product'][:60]}...")
                        else:
                            logger.info(f"      -> DISCARDED (Not relevant by AI): {product_details['Product'][:60]}...")
                    else:
                        logger.info(f"      -> DISCARDED (no quantity): {product_details['Product'][:60]}...")
        except Exception as e:
            logger.error(f"    ! Unexpected error occurred in Mumzworld scraper", exc_info=True)
        finally:
            if driver:
                driver.quit()

        return valid_products_found