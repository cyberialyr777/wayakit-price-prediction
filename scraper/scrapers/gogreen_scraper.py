import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
from utils import parse_volume_with_multiplier, parse_count_string
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from log_config import get_logger

logger = get_logger()

class GoGreenScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent
        self.base_url = "https://gogreen.com.sa/"
        self.products_to_find_limit = 5

    def _log(self, msg):
        logger.info(msg)

    def _safe_get_text(self, soup_element):
        return soup_element.get_text(strip=True) if soup_element else None

    def _set_language_to_english(self, driver):
        try:
            html_tag = driver.find_element(By.TAG_NAME, "html")
            if html_tag.get_attribute("lang") == "en":
                return True
            logger.info("    -> Changing language to English...")
            lang_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a.js-language-shipping"))
            )
            lang_button.click()
            language_select_element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.ID, "floatingSelectLanguage"))
            )
            select = Select(language_select_element)
            select.select_by_value("en")
            save_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.js-button-save"))
            )
            save_button.click()
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//html[@lang='en']"))
            )
            logger.info("    -> SUCCESS! Language verified correctly.")
            return True
        except Exception as e:
            logger.error(f"    -> ERROR: Could not set language to English.", exc_info=True)
            return False

    def _extract_product_details(self, driver, product_url, search_mode):
        details = {
            'Product': 'Not found', 'Price_SAR': '0.00', 'Company': 'GoGreen',
            'URL': product_url, 'Unit of measurement': 'units', 'Total quantity': 0
        }
        try:
            driver.get(product_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.h5")))
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            name_tag = soup.select_one("h1.h5")
            product_name = self._safe_get_text(name_tag)
            details['Product'] = product_name

            price_tag = soup.select_one("span.js-product-price")
            if price_tag:
                price_text = re.search(r'([\d,]+\.\d{2})', price_tag.get_text())
                if price_text:
                    details['Price_SAR'] = price_text.group(1).replace(',', '')

            if product_name:
                parsed_data = None
                if search_mode == 'units':
                    parsed_data = parse_count_string(product_name)
                else: 
                    parsed_data = parse_volume_with_multiplier(product_name)

                if parsed_data:
                    details['Total quantity'] = parsed_data['quantity']
                    details['Unit of measurement'] = parsed_data['unit']
                    logger.debug(f"        -> Quantity extracted: {details['Total quantity']} {details['Unit of measurement']}")

        except Exception as e:
            logger.error(f"      ! Error extracting details from {product_url}", exc_info=True)
        
        return details

    def scrape(self, keyword, search_mode):
        self._log(f"  [GoGreen Scraper] Buscando: '{keyword}' (Modo: {search_mode})")
        all_found_products = []

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
        # options.add_argument('--start-maximized')
        
        driver = webdriver.Chrome(service=service, options=options)

        try:
            driver.get(self.base_url)
            if not self._set_language_to_english(driver):
                if driver:
                    driver.quit()
                return []
            
            search_url = urljoin(self.base_url, f"products?search={quote(keyword)}")
            driver.get(search_url)

            WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.card.card-product")))
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            product_containers = soup.select("div.card.card-product")
            logger.debug(f"    -> Found {len(product_containers)} products on results page.")

            for container in product_containers:
                if len(all_found_products) >= self.products_to_find_limit:
                    break

                link_tag = container.select_one("a.css-thumbnail")
                if not link_tag or not link_tag.has_attr('href'):
                    continue
                
                product_url = urljoin(self.base_url, link_tag['href'])
                logger.debug(f"      -> Processing: {product_url[:80]}...")
                
                product_details = self._extract_product_details(driver, product_url, search_mode)
                
                if product_details.get('Total quantity', 0) > 0:
                    if self.relevance_agent.is_relevant(product_details.get('Product'), keyword):
                        all_found_products.append(product_details)
                        logger.info(f"      -> VALID PRODUCT SAVED: {product_details['Product'][:60]}...")
                    else:
                        logger.info(f"      -> DISCARDED (Not relevant by AI): {product_details['Product'][:60]}...")
                else:
                    logger.info(f"      -> DISCARDED (No valid quantity): {product_details['Product'][:60]}...")
            
        except TimeoutException:
            logger.warning("    -> No products found or page took too long to load.")
        except Exception as e:
            logger.error(f"    ! Unexpected error occurred in GoGreen scraper", exc_info=True)
        finally:
            if driver:
                driver.quit()

        return all_found_products