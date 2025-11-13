import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, ElementClickInterceptedException
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
from utils import parse_volume_string, parse_count_string
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from log_config import get_logger

logger = get_logger()


class FineScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent
        self.base_url = "https://ksa.finestore.com/en"
        self.products_to_find_limit = 5

    def _log(self, msg):
        logger.info(msg)

    def _safe_get_text(self, element):
        return element.get_text(strip=True) if element else None

    def _close_modal(self):
        try:
            js_script = 'document.querySelectorAll(".newsletter, .subscribe, .popup, .ecomz-popup, .modal, .overlay").forEach(function(e){e.style.display="none"});'
            self.driver.execute_script(js_script)
            time.sleep(0.3)
            return True
        except Exception:
            return False

    def _extract_price(self, soup):
        try:
            price_element = soup.select_one("div.ecomz-product-price-style")
            if price_element:
                text = price_element.get_text(separator=' ', strip=True)
                sar_match = re.search(r'SAR\s*([\d.,]+)', text, re.I)
                if sar_match:
                    return sar_match.group(1).replace(',', '')
        except Exception:
            pass
        return '0.00'

    def _extract_product_specs(self, soup):
        base_volume_data = None
        multiplier = 1
        
        description_rows = soup.select("div.product-tr")
        for row in description_rows:
            try:
                cells = row.select("div.product-cell")
                if len(cells) != 2:
                    continue
                    
                label = (self._safe_get_text(cells[0]) or '').lower()
                value = (self._safe_get_text(cells[1]) or '').strip()
                
                if not label or not value:
                    continue

                if ("ml" in label or "ml of" in label) and not base_volume_data:
                    base_volume_data = parse_volume_string(value)
                elif ("l" in label or "liters" in label or "ltr" in label) and not base_volume_data:
                    base_volume_data = parse_volume_string(value)

                if not any(term in label for term in ['number of sheets', 'number of wipes', 'number of pieces', 'volume', 'capacity', 'content', 'size']):
                    if not re.search(r'(\d+)\s*(ml|l|ltr|liter|liters|gallons?|fl\s?oz)\b', value, re.I):
                        m_pack = re.search(r'\b(\d+)\b', value)
                        if m_pack:
                            potential_multiplier = int(m_pack.group(1))
                            if 2 <= potential_multiplier <= 50:
                                multiplier = potential_multiplier
                            
            except Exception:
                continue
                
        return base_volume_data, multiplier

    def _extract_units_data(self, soup, product_title):
        description_rows = soup.select("div.product-tr")
        
        for row in description_rows:
            try:
                cells = row.select("div.product-cell")
                if len(cells) != 2:
                    continue
                    
                label = (self._safe_get_text(cells[0]) or '').lower()
                value = (self._safe_get_text(cells[1]) or '').strip()
                
                if any(term in label for term in ['number of sheets', 'number of wipes', 'number of pieces', 'count', 'pcs']):
                    match = re.search(r'(\d+)', value)
                    if match:
                        sheets_count = int(match.group(1))
                        return {'quantity': sheets_count, 'unit': 'units', 'normalized': sheets_count}
            except Exception:
                continue
        
        return parse_count_string(product_title)

    def _extract_title_multiplier(self, title):
        try:
            match = re.search(r'[x×]\s*(\d+)\b', title, re.I)
            if match:
                return int(match.group(1))
            
            match = re.search(r'\(carton of \d+x(\d+)\)', title, re.I)
            if match:
                return int(match.group(1))
            
            match = re.search(r'(\d+)\s+(bottles?|pieces?|units?|pcs?|pack)', title, re.I)
            if match:
                return int(match.group(1))
                
        except Exception:
            pass
        return 1

    def _extract_product_details(self, driver, product_url, search_mode):
        details = {
            'Product': 'Not found', 'Price_SAR': '0.00', 'Company': 'Fine',
            'URL': product_url, 'Unit of measurement': 'units', 'Total quantity': 0
        }
        
        try:
            for _ in range(2):
                try:
                    WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.ecomz-product-name-style"))
                    )
                    break
                except TimeoutException:
                    self._close_modal()
                    time.sleep(0.5)

            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.ecomz-product-price-style"))
                )
            except TimeoutException:
                pass

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            details['Product'] = (
                self._safe_get_text(soup.select_one("span.mg-l-0.f-xs-18")) or
                self._safe_get_text(soup.select_one("div.ecomz-product-name-style")) or
                self._safe_get_text(soup.select_one("h1")) or
                details['Product']
            )

            details['Price_SAR'] = self._extract_price(soup)

            if search_mode == 'units':
                final_data = self._extract_units_data(soup, details['Product'])
                _, spec_multiplier = self._extract_product_specs(soup)
                title_multiplier = self._extract_title_multiplier(details['Product'])
                multiplier = max(spec_multiplier, title_multiplier)
            else:
                base_volume_data, multiplier = self._extract_product_specs(soup)
                title_multiplier = self._extract_title_multiplier(details['Product'])
                multiplier = max(multiplier, title_multiplier)
                
                final_data = (base_volume_data or 
                             parse_volume_string(details['Product']) or 
                             parse_count_string(details['Product']))

            if final_data:
                details['Unit of measurement'] = final_data['unit']
                details['Total quantity'] = final_data['quantity'] * multiplier

        except Exception as e:
            logger.error(f"      ! Error extracting details from {product_url}", exc_info=True)

        return details

    def _is_valid_product(self, product_details):
        try:
            price_val = float(product_details.get('Price_SAR', '0').replace(',', ''))
            if price_val <= 0:
                return False, "Invalid price"
        except (ValueError, AttributeError):
            return False, "Invalid price format"
        
        if product_details.get('Total quantity', 0) <= 0:
            return False, "Invalid quantity"
            
        if product_details.get('Product', '') == 'Not found':
            return False, "Product name not found"
            
        return True, "Valid"

    def _navigate_to_product(self, driver, link_element, href):
        for attempt in range(2):
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link_element)
                time.sleep(0.4)
                
                try:
                    link_element.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", link_element)
                
                WebDriverWait(driver, 12).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.ecomz-product-name-style"))
                )
                return True
                
            except Exception:
                if attempt == 0:
                    self._close_modal()
                    if href:
                        try:
                            absolute_url = urljoin(self.base_url, href)
                            driver.get(absolute_url)
                            WebDriverWait(driver, 12).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.ecomz-product-name-style"))
                            )
                            return True
                        except Exception:
                            continue
                
        return False

    def scrape(self, keyword, search_mode):
        self._log(f"  [Fine Scraper] Searching for: '{keyword}' (Mode: {search_mode})")
        
        search_url = f"{self.base_url}/products?keyword={quote(keyword)}"
        all_found_products = []
        page_num = 1

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
        
        self.driver = webdriver.Chrome(service=service, options=options)

        try:
            self.driver.get(search_url)
        except Exception as e:
            logger.error(f"    ! Error loading search URL", exc_info=True)
            return []

        while len(all_found_products) < self.products_to_find_limit:
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.listing-page a.display-flex"))
                )
                
                search_page_url = self.driver.current_url
                time.sleep(1.5)

                link_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.listing-page a.display-flex")
                num_products = len(link_elements)
                
                logger.debug(f"      -> Found {num_products} products on search page")
                
                if num_products == 0:
                    break

                for i in range(num_products):
                    if len(all_found_products) >= self.products_to_find_limit:
                        break
                        
                    logger.debug(f"      -> Processing product {i+1}/{num_products}")
                        
                    try:
                        fresh_links = self.driver.find_elements(By.CSS_SELECTOR, "div.listing-page a.display-flex")
                        if i >= len(fresh_links):
                            break
                            
                        link_element = fresh_links[i]
                        href = link_element.get_attribute('href')

                        if not self._navigate_to_product(self.driver, link_element, href):
                            logger.warning(f"      -> Could not navigate to product")
                            continue

                        self._close_modal()
                        product_url = self.driver.current_url
                        product_details = self._extract_product_details(self.driver, product_url, search_mode)
                        
                        logger.debug(f"      -> Extracted: {product_details['Product'][:50]}... | Price: {product_details['Price_SAR']} | Qty: {product_details['Total quantity']}")

                        is_valid, validation_msg = self._is_valid_product(product_details)
                        if not is_valid:
                            logger.info(f"      -> Validation failed: {validation_msg}")
                            continue

                        logger.debug(f"      -> Checking AI relevance...")
                        if self.relevance_agent.is_relevant(product_details.get('Product'), keyword):
                            all_found_products.append(product_details)
                            logger.info(f"      -> Product found: {product_details['Product'][:60]}...")
                        else:
                            logger.info(f"      -> AI rejected product")

                    except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException):
                        continue
                    finally:
                        try:
                            self.driver.get(search_page_url)
                            WebDriverWait(self.driver, 8).until(
                                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.listing-page a.display-flex"))
                            )
                            time.sleep(0.5)
                        except Exception:
                            pass

                try:
                    next_page_button = self.driver.find_element(By.XPATH, "//a[contains(text(), 'Next')]")
                    self.driver.execute_script("arguments[0].click();", next_page_button)
                    page_num += 1
                except NoSuchElementException:
                    break

            except TimeoutException:
                break
            except Exception as e:
                logger.error(f"    ! Unexpected error", exc_info=True)
                break
        
        if self.driver:
            self.driver.quit()

        return all_found_products
