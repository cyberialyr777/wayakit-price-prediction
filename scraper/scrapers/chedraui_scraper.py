from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin, quote
from utils import separate_title_and_volume, separate_title_and_units
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from log_config import get_logger
import time
from selenium.common.exceptions import TimeoutException

logger = get_logger()

class ChedrauiScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent
        self.base_url = "https://www.chedraui.com.mx"

    def _log(self, msg):
        logger.info(msg)

    def _safe_get_text(self, element):
        return element.get_text(strip=True) if element else None

    def _extract_details_from_product_page(self, soup, search_mode, keyword, product_url):
        """Extract product details from a Chedraui PDP (Product Detail Page)."""
        details = {
            'Product': None, 'Price_SAR': '0.00', 'Company': 'Company not found',
            'Unit of measurement': 'units', 'Total quantity': 0, 'Validation_Status': 'Not Found',
            'URL': product_url
        }

        # Title: first span inside h1 with class vtex-store-components-3-x-productNameContainer
        title_h1 = soup.find('h1', class_=lambda c: c and 'vtex-store-components-3-x-productNameContainer' in c)
        if title_h1:
            # Get the first span (brand name = clean title with volume)
            first_span = title_h1.find('span')
            if first_span:
                details['Product'] = first_span.get_text(strip=True)

        # Brand
        brand_span = soup.find('span', class_=lambda c: c and 'vtex-store-components-3-x-productBrandName' in c)
        details['Company'] = self._safe_get_text(brand_span) or details['Company']

        # Price
        price_span = soup.find('span', class_=lambda c: c and 'chedrauimx-products-simulator-0-x-simulatedSellingPrice' in c)
        if price_span:
            price_text = self._safe_get_text(price_span)
            if price_text:
                price_clean = price_text.replace('$', '').replace(',', '').strip()
                details['Price_SAR'] = price_clean

        raw_title = details.get('Product')
        if not raw_title:
            return details

        # Volume / Units Parsing
        if search_mode == 'units':
            self._log("      [Extractor] Mode: Units")
            parsed = separate_title_and_units(raw_title)
            if parsed and parsed.get('unit_data'):
                unit_data = parsed['unit_data']
                details['Product'] = parsed['title']
                details['Total quantity'] = unit_data['quantity']
                details['Unit of measurement'] = unit_data['unit']
                details['Validation_Status'] = 'From Title (Regex)'
        elif search_mode == 'volume':
            self._log("      [Extractor] Mode: Volume")
            parsed = separate_title_and_volume(raw_title)
            if parsed and parsed.get('volume_data'):
                vol_data = parsed['volume_data']
                details['Product'] = parsed['title']
                details['Total quantity'] = vol_data['quantity']
                details['Unit of measurement'] = vol_data['unit']
                details['Validation_Status'] = 'From Title (Regex)'

        if details.get('Total quantity', 0) > 0:
            logger.debug(f"      [Extractor] ✅ Quantity found: {details['Total quantity']} {details['Unit of measurement']} (Source: {details['Validation_Status']})")
        else:
            logger.debug("      [Extractor] ❌ No valid quantity found on page.")

        return details

    def scrape(self, keyword, search_mode):
        self._log(f"   [Chedraui Scraper] Searching: '{keyword}' (Mode: {search_mode})")
        found_products = []
        products_to_find = 20

        # Chedraui search URL pattern
        encoded_keyword = quote(keyword)
        search_url = f"{self.base_url}/{encoded_keyword}?_q={encoded_keyword}&map=ft"

        service = ChromeService(executable_path=self.driver_path)
        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-webgl')
        options.add_argument('--disable-3d-apis')
        options.add_argument(f"user-agent={config.USER_AGENT}")
        options.add_argument('--log-level=3')
        options.add_argument('--no-sandbox')
        options.add_argument('--headless') 
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-browser-side-navigation')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)

        try:
            try:
                driver.get(search_url)
            except TimeoutException:
                self._log("      ! Page load timed out. Skipping this search.")
                driver.quit()
                return found_products
            time.sleep(5)  # VTEX sites need extra time for JS rendering

            page_num = 1
            while len(found_products) < products_to_find:
                self._log(f"      [Page {page_num}] Scanning results...")

                # Wait for product containers to load
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='chedrauimx-search-result-3-x-galleryItem']"))
                    )
                except Exception:
                    self._log("      ! Timed out waiting for product containers. Attempting to parse anyway.")

                # Scroll down to load all products on the page
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                soup = BeautifulSoup(driver.page_source, 'html.parser')

                # Find product containers
                product_containers = soup.find_all('div', class_=lambda c: c and 'chedrauimx-search-result-3-x-galleryItem' in c)

                if not product_containers:
                    self._log("      ! No products found on this page.")
                    break

                self._log(f"      Found {len(product_containers)} product containers on page {page_num}")

                # Collect all product URLs from the PLP first
                product_urls = []
                for container in product_containers:
                    link_tag = container.find('a', class_=lambda c: c and 'vtex-product-summary-2-x-clearLink' in c)
                    if link_tag and link_tag.get('href'):
                        product_url = urljoin(self.base_url, link_tag['href'])
                        product_urls.append(product_url)

                if not product_urls:
                    self._log("      ! No product links found on this page.")
                    break

                # Visit each PDP
                for product_url in product_urls:
                    if len(found_products) >= products_to_find:
                        break

                    self._log(f"      > Visiting product page: {product_url[:100]}...")
                    try:
                        driver.get(product_url)
                    except TimeoutException:
                        logger.warning("      ! Product page timed out, skipping.")
                        continue

                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "h1[class*='vtex-store-components-3-x-productNameContainer']"))
                        )
                    except Exception:
                        logger.warning("      ! Product title not found on PDP, skipping.")
                        continue

                    # Wait for the price element to render (JS-loaded)
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "span[class*='simulatedSellingPrice']"))
                        )
                    except Exception:
                        logger.warning("      ! Price element not loaded, will try to parse anyway.")

                    time.sleep(2)
                    product_soup = BeautifulSoup(driver.page_source, 'html.parser')
                    product_details = self._extract_details_from_product_page(product_soup, search_mode, keyword, product_url)

                    product_title = product_details.get('Product')
                    if not product_title:
                        logger.warning("      -> DISCARDED (No title found)")
                        continue

                    # Filter: Must have quantity > 0 before calling AI
                    if product_details.get('Total quantity', 0) > 0:
                        # Deduplication Check
                        if any(p['Product'] == product_title for p in found_products):
                            self._log(f"      -> Duplicate detected. Skipping: {product_title[:40]}...")
                            continue

                        is_relevant = self.relevance_agent.is_relevant(product_title, keyword)
                        time.sleep(1)

                        if is_relevant:
                            found_products.append(product_details)
                            logger.info(f"      -> ✅ RELEVANT & VALID. Product saved. ({len(found_products)}/{products_to_find})")
                        else:
                            logger.info(f"      -> DISCARDED (Not relevant by AI): {product_title[:60]}...")
                    else:
                        logger.info(f"      -> DISCARDED (No quantity found): {product_title[:60]}...")

                    if len(found_products) >= products_to_find:
                        break

                if len(found_products) >= products_to_find:
                    break

                # Pagination Logic
                # Navigate back to search results page to check for next page
                # Reconstruct the search URL with page parameter
                next_page_num = page_num + 1
                next_page_url = f"{self.base_url}/{encoded_keyword}?_q={encoded_keyword}&map=ft&page={next_page_num}"

                self._log(f"      [Pagination] Checking for page {next_page_num}...")

                # Before navigating, check if current page had pagination controls
                # Re-fetch the PLP to check pagination (we navigated away to PDPs)
                driver.get(search_url if page_num == 1 else f"{self.base_url}/{encoded_keyword}?_q={encoded_keyword}&map=ft&page={page_num}")
                time.sleep(3)

                plp_soup = BeautifulSoup(driver.page_source, 'html.parser')
                pagination_section = plp_soup.find('section', class_=lambda c: c and 'ContainerPaginationNew' in c)

                if not pagination_section:
                    self._log("      [Pagination] No pagination controls found. Stopping.")
                    break

                # Check if "Siguiente" (Next) button is active
                next_button = pagination_section.find('a', class_=lambda c: c and 'ButtonNext' in c)
                if next_button:
                    next_classes = next_button.get('class', [])
                    is_active = any('perPageActive' in cls for cls in next_classes)
                    if not is_active:
                        self._log("      [Pagination] Next button is inactive. No more pages.")
                        break
                else:
                    self._log("      [Pagination] Next button not found. Stopping.")
                    break

                # Navigate to the next page
                self._log(f"      [Pagination] Going to page {next_page_num}...")
                driver.get(next_page_url)
                page_num = next_page_num
                time.sleep(4)

        except Exception as e:
            logger.error(f"      ! Unexpected error occurred in Chedraui scraper", exc_info=True)
        finally:
            if driver:
                driver.quit()

        return found_products
