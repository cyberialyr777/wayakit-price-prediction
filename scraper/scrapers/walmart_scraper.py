from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
from utils import separate_title_and_volume, separate_title_and_units
import config
import undetected_chromedriver as uc
# from selenium.webdriver.chrome.service import Service as ChromeService 
# from webdriver_manager.chrome import ChromeDriverManager 
from log_config import get_logger
import time

logger = get_logger()

class WalmartScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path # UC deals with driver path, but we can pass it if we want specific version management or let uc handle it.
        # usually uc.Chrome() handles download. validation: user has existing driver management in main.py. 
        # We might ignore driver_path from main.py if we let uc handle it, or try to use it. 
        # Best practice with uc is letting it manage, or passing executable_path.
        self.relevance_agent = relevance_agent
        self.base_url = "https://www.walmart.com.mx"

    def _log(self, msg):
        logger.info(msg)

    def _safe_get_text(self, element):
        return element.get_text(strip=True) if element else None

    def _extract_details_from_product_page(self, soup, keyword):
        details = {
            'Product': None, 'Price_SAR': '0.00', 'Company': 'Company not found',
            'Unit of measurement': 'units', 'Total quantity': 0, 'Validation_Status': 'Not Found',
            'URL': None
        }

        # Extract Title
        # User provided: <h1 ... data-fs-element="name" ...>
        title_elem = soup.find('h1', {'data-fs-element': 'name'})
        if not title_elem:
            title_elem = soup.find('h1', id='main-title') # Fallback
        
        raw_title = self._safe_get_text(title_elem)
        details['Product'] = raw_title

        # Extract Brand
        # User provided: <a data-dca-name="ItemBrandLink" ...>
        brand_elem = soup.find('a', {'data-dca-name': 'ItemBrandLink'})
        if brand_elem:
            details['Company'] = self._safe_get_text(brand_elem)

        # Extract Price
        # User provided: <span itemprop="price" data-seo-id="hero-price" data-fs-element="price" ...>
        price_elem = soup.find('span', {'data-fs-element': 'price'})
        price_text = self._safe_get_text(price_elem)
        
        if price_text:
            # Remove '$' and commas
            clean_price = price_text.replace('$', '').replace(',', '').strip()
            details['Price_SAR'] = clean_price

        if not raw_title:
            return details

        # Parse Volume/Units using utils.py function
        extracted_data = separate_title_and_volume(raw_title)
        volume_data = extracted_data.get('volume_data')
        
        if volume_data:
            details['Total quantity'] = volume_data['quantity']
            details['Unit of measurement'] = volume_data['unit']
            details['Validation_Status'] = 'From Title (Volume)'
            # Update 'Product' to the cleaner title
            details['Product'] = extracted_data.get('title', raw_title) 
        else:
            # Try parsing units (wipes, pieces, count, etc.)
            extracted_units_data = separate_title_and_units(raw_title)
            unit_data = extracted_units_data.get('unit_data')
            
            if unit_data:
                details['Total quantity'] = unit_data['quantity']
                details['Unit of measurement'] = unit_data['unit']
                details['Validation_Status'] = 'From Title (Units)'
                details['Product'] = extracted_units_data.get('title', raw_title)
            else:
                 pass

        return details

    def scrape(self, keyword, search_mode):
        self._log(f"   [Walmart Scraper] Searching: '{keyword}'")
        found_products = []
        products_to_find = 1
        search_url = f"{self.base_url}/search?q={keyword.replace(' ', '+')}"

        options = uc.ChromeOptions()
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        # options.add_argument('--headless') 

        driver = uc.Chrome(options=options, version_main=146)

        try:
            driver.get(search_url)
            time.sleep(3)
            
            while len(found_products) < products_to_find:
                # Wait for search results
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/ip/']"))
                    )
                except Exception:
                    self._log("      ! No products found on this page.")
                    break
                
                # Scroll down to ensure lazy-loaded items appear
                self._log("      > Scrolling to load more items...")
                for _ in range(3):
                    driver.execute_script("window.scrollBy(0, 1000);")
                    time.sleep(1)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                soup = BeautifulSoup(driver.page_source, 'html.parser')

                # Pre-extract Next Page URL using Selenium for better reliability
                next_page_url = None
                try:
                    # Look for the nav element first to ensure footer is loaded
                    pagination_nav = driver.find_elements(By.CSS_SELECTOR, "nav[aria-label='paginación']")
                    
                    # Try finding the next page button
                    next_elements = driver.find_elements(By.CSS_SELECTOR, "a[data-testid='NextPage']")
                    if not next_elements:
                         next_elements = driver.find_elements(By.CSS_SELECTOR, "a[aria-label='Página siguiente']")
                    
                    if next_elements:
                        next_url_raw = next_elements[0].get_attribute('href')
                        if next_url_raw:
                            if next_url_raw.startswith('http'):
                                next_page_url = next_url_raw
                            else:
                                next_page_url = urljoin(self.base_url, next_url_raw)
                            self._log(f"      > Next page found: {next_page_url}")
                except Exception as e:
                    pass
                
                # Find all product links
                product_links = soup.find_all('a', href=lambda x: x and '/ip/' in x)
                
                # Deduplicate links
                seen_urls = set()
                unique_links = []
                for link in product_links:
                    href = link.get('href')
                    if not href: continue
                    full_url = urljoin(self.base_url, href)
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        unique_links.append(full_url)

                self._log(f"      > Found {len(unique_links)} products on this page.")

                for product_url in unique_links:
                    if len(found_products) >= products_to_find:
                        break

                    self._log(f"      > Visiting product page: {product_url[:100]}...")
                    driver.get(product_url)
                    
                    try:
                        WebDriverWait(driver, 10).until(EC.any_of(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "h1[data-fs-element='name']")),
                            EC.presence_of_element_located((By.ID, "main-title")),
                            EC.presence_of_element_located((By.CSS_SELECTOR, "span[data-fs-element='price']"))
                        ))
                    except Exception:
                        logger.warning("      ! Product page details not found or timed out, skipping.")
                        continue

                    product_soup = BeautifulSoup(driver.page_source, 'html.parser')
                    product_details = self._extract_details_from_product_page(product_soup, keyword)
                    product_details['URL'] = product_url
                    
                    product_title = product_details.get('Product')
                    
                    if not product_title:
                        logger.warning(f"      -> DISCARDED (No title found)")
                        continue

                    if product_details.get('Total quantity', 0) > 0:
                        is_relevant = self.relevance_agent.is_relevant(product_title, keyword)
                        
                        if is_relevant:
                            found_products.append(product_details)
                            logger.info(f"      -> ✅ RELEVANT & VALID: {product_title[:60]}... ({product_details['Total quantity']} {product_details['Unit of measurement']})")
                        else:
                            logger.info(f"      -> DISCARDED (Not relevant by AI): {product_title[:60]}...")
                    else:
                        logger.info(f"      -> DISCARDED (No quantity found): {product_title[:60]}...")
                    
                    time.sleep(1)
                
                # Use pre-extracted next page URL
                if len(found_products) < products_to_find:
                    if next_page_url:
                        self._log(f"   [Pagination] Moving to next page: {next_page_url}")
                        driver.get(next_page_url)
                        time.sleep(3)
                    else:
                        self._log("   [Pagination] No next page URL available. Stopping.")
                        break
                else:
                    break

        except Exception as e:
            logger.error(f"      ! Unexpected error occurred in Walmart scraper", exc_info=True)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        return found_products
