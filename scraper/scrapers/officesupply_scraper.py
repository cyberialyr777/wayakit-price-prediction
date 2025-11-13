import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

class OfficeSupplyScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent
        self.base_url = "https://officesupply.sa/en/"
        self.products_to_find_limit = 8

    def _log(self, msg):
        logger.info(msg)

    def _extract_price(self, soup):
        container = soup.select_one('span.ty-price bdi, .ty-price bdi') or soup.select_one('h1 bdi')
        if not container:
            return None
        spans = container.select('span.ty-price-num')
        # Filtrar spans con dígitos
        digit_spans = [s for s in spans if re.search(r'\d', s.get_text())]
        # Caso típico: primer span icono, segundo span valor con <sup>
        target = None
        if len(digit_spans) >= 1:
            target = digit_spans[0]
        elif spans:
            target = spans[-1]
        if not target:
            return None
        sup = target.find('sup')
        full_digits = ''.join(re.findall(r'\d+', target.get_text()))
        dec = ''.join(re.findall(r'\d+', sup.get_text())) if sup else ''
        if sup and dec and full_digits.endswith(dec):
            integer_part = full_digits[:-len(dec)] or full_digits
        else:
            integer_part = full_digits
        if not integer_part:
            return None
        price = f"{integer_part}.{dec}" if dec else integer_part
        # Normalizar: asegurar 2 decimales
        if re.match(r'^\d+$', price):
            price = price + '.00'
        return price

    def _extract_product_details(self, driver, product_url, search_mode):
        logger.debug(f"        -> Extracting details from: {product_url}")
        details = {
            'Product': 'Not found', 'Price_SAR': '0.00', 'Company': 'Brand not found',
            'URL': product_url, 'Unit of measurement': 'units', 'Total quantity': 0
        }

        try:
            driver.get(product_url)
            
            logger.debug("        -> Waiting for product price to load...")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.ty-price-num"))
            )
            logger.debug("        -> Price found! Extracting data.")
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            name_tag = soup.select_one("div.ut2-pb__title h1")
            if name_tag:
                product_title = name_tag.get_text(strip=True)
                details['Product'] = product_title
                logger.debug(f"        -> TITLE EXTRACTED: '{product_title}'")

            extracted_price = self._extract_price(soup)
            if extracted_price:
                details['Price_SAR'] = extracted_price
                logger.debug(f"        -> Final extracted price: {details['Price_SAR']}")
            else:
                logger.warning("        -> WARNING: Could not extract price (helper). Partial HTML captured.")

            if details['Product'] != 'Not found':
                parsed_data = None
                if search_mode == 'units':
                    parsed_data = parse_count_string(details['Product'])
                else: 
                    parsed_data = parse_volume_with_multiplier(details['Product'])

                if parsed_data:
                    details['Total quantity'] = parsed_data['quantity']
                    details['Unit of measurement'] = parsed_data['unit']
                    logger.debug(f"        -> Quantity extracted: {details['Total quantity']} {details['Unit of measurement']}")
                else:
                    logger.warning("        -> ALERT: Parse function did not find a valid quantity.")


        except TimeoutException:
            logger.warning("      ! Error: Price not found on page after 15 seconds.")
        except Exception as e:
            logger.error(f"      ! Unexpected error extracting details", exc_info=True)
        
        return details


    def scrape(self, keyword, search_mode):
        self._log(f"  [OfficeSupply Scraper] Buscando: '{keyword}' (Modo: {search_mode})")
        
        search_keyword = quote(keyword)
        search_url = (
            f"{self.base_url}?match=all&subcats=Y&pcode_from_q=Y&pshort=Y&pfull=Y"
            f"&pname=Y&pkeywords=Y&search_performed=Y&dispatch=products.search&q={search_keyword}"
        )
        
        all_found_products = []

        service = ChromeService(executable_path=self.driver_path)
        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-notifications')
        options.add_argument('--headless')
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
            driver.get(search_url)

            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.ut2-gl__body"))
            )
            
            logger.info("    > Search results page loaded.")
            
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            product_containers = soup.select("div.ut2-gl__body")
            logger.debug(f"    > Found {len(product_containers)} products on page.")

            product_urls = []
            for container in product_containers:
                link_tag = container.select_one("a.product_icon_lnk")
                if link_tag and link_tag.has_attr('href'):
                    product_urls.append(urljoin(self.base_url, link_tag['href']))

            logger.debug(f"    > Found {len(product_urls)} product URLs to process.")

            for product_url in product_urls:
                if len(all_found_products) >= self.products_to_find_limit:
                    logger.info(f"    > Limit of {self.products_to_find_limit} products reached.")
                    break
                
                product_details = self._extract_product_details(driver, product_url, search_mode)
                
                if product_details.get('Total quantity', 0) > 0:
                    is_relevant = self.relevance_agent.is_relevant(product_details.get('Product'), keyword)
                    if is_relevant:
                        all_found_products.append(product_details)
                        logger.info(f"      -> VALID PRODUCT SAVED: {product_details['Product'][:60]}...")
                    else:
                        logger.info(f"      -> DISCARDED (Not relevant by AI): {product_details['Product'][:60]}...")
                else:
                    logger.info(f"      -> DISCARDED (No valid quantity): {product_details.get('Product', 'N/A')[:60]}...")

        except TimeoutException:
            logger.warning("    > No products found or page took too long to load.")
        except Exception as e:
            logger.error(f"    ! Unexpected error occurred during OfficeSupply search", exc_info=True)
        finally:
            if driver:
                driver.quit()

        return all_found_products