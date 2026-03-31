from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
from utils import separate_title_and_volume, separate_title_and_units
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from log_config import get_logger
import time

logger = get_logger()

class SorianaScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent 
        self.base_url = "https://www.soriana.com"

    def _log(self, msg):
        logger.info(msg)

    def _safe_get_text(self, element):
        return element.get_text(strip=True) if element else None

    def _extract_details_from_product_page(self, soup, search_mode, keyword, product_url):
        details = {
            'Product': None, 'Price_SAR': '0.00', 'Company': 'Company not found',
            'Unit of measurement': 'units', 'Total quantity': 0, 'Validation_Status': 'Not Found',
            'URL': product_url
        }

        # Title
        title_tag = soup.find('h1', class_='product-name')
        details['Product'] = self._safe_get_text(title_tag)
        
        # Brand
        brand_tag = soup.find('p', class_='brand-product')
        details['Company'] = self._safe_get_text(brand_tag) or details['Company']
        
        # Price extraction
        # User snippet: <span class="mr-0 cart-price ..."><span class="">$60.50</span></span>
        price_tag = soup.find('span', class_='cart-price')
        price_text = None
        if price_tag:
            price_text = self._safe_get_text(price_tag)
        
        if not price_text:
             # Try finding by class 'price-pdp' if cart-price didn't give text (or wasn't found)
             price_tag = soup.find('span', class_='price-pdp')
             if price_tag:
                 price_text = self._safe_get_text(price_tag)

        if price_text:
            # Clean up: remove '$', commas, extra spaces, line breaks
            price_clean = price_text.replace('$', '').replace(',', '').strip()
            details['Price_SAR'] = price_clean

        raw_title = details.get('Product')
        if not raw_title:
            return details 

        # Volume / Units Parsing
        if search_mode == 'units':
            self._log("      [Extractor] Mode: Units")
            # Logic: Use utils.separate_title_and_units
            parsed = separate_title_and_units(raw_title)
            if parsed and parsed.get('unit_data'):
                unit_data = parsed['unit_data']
                details['Total quantity'] = unit_data['quantity']
                details['Unit of measurement'] = unit_data['unit'] # 'units'
                details['Validation_Status'] = 'From Title (Regex)'
            else:
                # Fallback to AI if needed? User didn't strictly say so for Soriana but Amazon does.
                # User said: "vas a usar la misma arquitectura q @[scraper/scrapers/amazon_scraper.py]"
                # Amazon scraper uses AI for wipes/rags. 
                # Soriana Prompt says: "usa la funcion correspondiente en @[scraper/utils.py] para separar el titulo con el volumen lo mismo para unidades"
                # It doesn't explicitly mention AI for units in the Soriana prompt, but implies "same architecture".
                # For now, I will stick to the utils functions as explicitly requested for Soriana specific instructions.
                # If utils fail, I might try AI if I follow Amazon architecture strictly.
                # Let's check if 'wipes' or 'rags' (or spanish equivalents) are in keyword.
                # Actually, the user specifically pointed to utils functions. I will try those first.
                pass

        elif search_mode == 'volume':
            self._log("      [Extractor] Mode: Volume")
            # Logic: Use utils.separate_title_and_volume
            parsed = separate_title_and_volume(raw_title)
            if parsed and parsed.get('volume_data'):
                vol_data = parsed['volume_data']
                details['Total quantity'] = vol_data['quantity']
                details['Unit of measurement'] = vol_data['unit']
                details['Validation_Status'] = 'From Title (Regex)'

        
        if details.get('Total quantity', 0) > 0:
            logger.debug(f"      [Extractor] ✅ Quantity found: {details['Total quantity']} {details['Unit of measurement']} (Source: {details['Validation_Status']})")
        else:
            logger.debug("      [Extractor] ❌ No valid quantity found on page.")
            
        return details
    
    def scrape(self, keyword, search_mode):
        self._log(f"   [Soriana Scraper] Searching: '{keyword}' (Mode: {search_mode})")
        found_products = []
        products_to_find = 15 # User said "si hay q buscar 15 productos"
        
        # User provided a complex URL but also a base one.
        # "https://www.soriana.com/buscar?productSuggestions__category=...&q=desinfectante..."
        # I'll use the standard search query param.
        search_url = f"{self.base_url}/buscar?q={keyword.replace(' ', '%20')}"

        service = ChromeService(executable_path=self.driver_path)
        options = webdriver.ChromeOptions()
        # Anti-detection: merge excludeSwitches into a single call
        options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-webgl')
        options.add_argument('--disable-3d-apis')
        options.add_argument(f"user-agent={config.USER_AGENT}")
        options.add_argument('--log-level=3')
        options.add_argument('--no-sandbox')
        # options.add_argument('--headless=new')  # New headless mode — harder for anti-bot to detect
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        driver = webdriver.Chrome(service=service, options=options)
        # Hide navigator.webdriver flag from JavaScript detection
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
        })

        try:
            driver.get(search_url)
            time.sleep(5)  # Give more time for Cloudflare challenge to pass
            
            # Pagination loop
            page_num = 1
            while len(found_products) < products_to_find:
                self._log(f"      [Page {page_num}] Scanning results...")
                
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "product-tile--wrapper")))
                soup = BeautifulSoup(driver.page_source, 'html.parser')

                # Check for pagination status
                next_button = soup.select_one('button.slick-next')
                has_next_page = next_button and 'slick-disabled' not in next_button.get('class', [])
                
                product_containers = soup.find_all('div', class_='product-tile--wrapper')

                if not product_containers:
                    self._log("      ! No products found on this page.")
                    break

                for container in product_containers:
                    if len(found_products) >= products_to_find:
                        break

                    link_tag = container.find('a', class_='plp-link')
                    if not link_tag:
                        continue
                    
                    product_url = urljoin(self.base_url, link_tag['href'])
                    
                    # Optimization: Check if we can get basic info from PLP to avoid loading PDP if obviously irrelevant?
                    # Amazon scraper loads PDP. User instruction: "Visit each PDP to extract..."
                    # So I will visit PDP.
                    
                    self._log(f"      > Visiting product page: {product_url[:100]}...")
                    driver.get(product_url)
                    time.sleep(3)  # Wait for page to settle / anti-bot challenge
                    
                    try:
                        # Wait for title using CSS selector (more reliable with multi-class elements)
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.product-name"))
                        )
                    except Exception:
                        # Retry: dismiss cookie banner if present, then wait again
                        try:
                            cookie_btn = driver.find_element(By.CSS_SELECTOR, "button.affirm, button[id*='accept'], button[class*='accept']")
                            if cookie_btn:
                                cookie_btn.click()
                                time.sleep(2)
                        except Exception:
                            pass
                        
                        # Check if we got blocked
                        page_src = driver.page_source.lower()
                        if 'you have been blocked' in page_src or 'access denied' in page_src:
                            logger.warning("      ! Anti-bot block detected on PDP. Waiting and retrying...")
                            time.sleep(5)
                            driver.get(product_url)
                            time.sleep(5)
                        
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.product-name"))
                            )
                        except Exception:
                            logger.warning("      ! Product title not found on PDP after retry, skipping.")
                            pass
                    
                    product_soup = BeautifulSoup(driver.page_source, 'html.parser')
                    product_details = self._extract_details_from_product_page(product_soup, search_mode, keyword, product_url)
                    
                    product_title = product_details.get('Product')
                    if not product_title:
                        logger.warning(f"      -> DISCARDED (No title found)")
                        # driver.back() # Not needed if we reload PLP or just go to next URL
                        continue

                    # Relevance Check
                    if product_details.get('Total quantity', 0) > 0:
                        is_relevant = self.relevance_agent.is_relevant(product_title, keyword)
                        time.sleep(1) # Be nice

                        if is_relevant:
                            found_products.append(product_details)
                            logger.info(f"      -> ✅ RELEVANT & VALID. Product saved. ({len(found_products)}/{products_to_find})")
                        else:
                            logger.info(f"      -> DISCARDED (Not relevant by AI): {product_title[:60]}...")
                    else:
                        logger.info(f"      -> DISCARDED (No quantity found): {product_title[:60]}...")
                    
                    # Check if we have enough
                    if len(found_products) >= products_to_find:
                        break
                
                if len(found_products) >= products_to_find:
                    break

                # Pagination Logic
                # We need to go to the next page. 
                # Since we navigated away to PDPs, we are strictly speaking on the LAST PDP visited.
                # We need to return to the PLP to click "Next" or construct the next page URL.
                # The user prompt shows pagination buttons with data-url.
                # Constructing URL might be safer/easier.
                # HTML snippet: <button class="slick-next ... " data-url="/on/demandware.store/Sites-Soriana-Site/default/Search-UpdateGrid?q=...&start=24...">
                # It seems 'start' increments by 24.
                # I can construct the next page URL if I know the pattern.
                # Or I can just go back to search_url and append paging parameters.
                # But 'search_url' was just the base. 
                # Let's try to deduce the next page URL or go back to PLP.
                
                self._log("      [Pagination] Returning to PLP to find next page...")
                # We can't just 'back' multiple times safely.
                # Let's reload the current PLP page first?
                # Actually, simpler: just increment a counter and modify the URL?
                # Params: q=keyword, start=0, sz=24 (page size).
                # Page 2 start=24, Page 3 start=48.
                # So next page is start = page_num * 24. (If 0-indexed, page 1 is 0, page 2 is 24).
                
                if not has_next_page:
                    self._log("      [Pagination] No more pages available. Stopping.")
                    break

                next_page_start = page_num * 24
                next_page_url = f"{self.base_url}/buscar?q={keyword.replace(' ', '%20')}&start={next_page_start}&sz=24"
                self._log(f"      [Pagination] Going to next page: {next_page_url}")
                driver.get(next_page_url)
                page_num += 1
                
                # Verify if valid page by checking if products exist (done at start of loop)
                
        except Exception as e:
            logger.error(f"      ! Unexpected error occurred in Soriana scraper", exc_info=True)
        finally:
            if driver:
                driver.quit()

        return found_products
