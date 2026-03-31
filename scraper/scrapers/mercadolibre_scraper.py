from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
from utils import parse_volume_string, parse_count_string
import config
import undetected_chromedriver as uc
from log_config import get_logger
import time
import os
import random

logger = get_logger()

# List of modern User Agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
]

class MercadoLibreScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent
        self.base_url = "https://www.mercadolibre.com.mx"

    def _log(self, msg):
        logger.info(msg)

    def _safe_get_text(self, element):
        return element.get_text(strip=True) if element else None

    def _extract_details_from_product_page(self, soup, search_mode, keyword):
        details = {
            'Product': None, 'Price_SAR': '0.00', 'Company': 'Company not found',
            'Unit of measurement': 'units', 'Total quantity': 0, 'Validation_Status': 'Not Found'
        }

        # 1. Product Title
        title_tag = soup.find('h1', class_='ui-pdp-title')
        raw_title = self._safe_get_text(title_tag)
        if raw_title:
            details['Product'] = raw_title
        
        # 2. Price Extraction
        price_meta = soup.find('meta', itemprop='price')
        if price_meta and price_meta.get('content'):
            details['Price_SAR'] = price_meta['content']
        else:
            price_container = soup.find('span', class_='ui-pdp-price__part')
            if price_container:
                fraction_span = price_container.find('span', class_='andes-money-amount__fraction')
                cents_span = price_container.find('span', class_='andes-money-amount__cents')
                
                fraction = self._safe_get_text(fraction_span)
                cents = self._safe_get_text(cents_span)
                
                if fraction:
                    price_str = fraction
                    if cents:
                        price_str += f".{cents}"
                    details['Price_SAR'] = price_str

        # 3. Volume / Unit Extraction
        if not details['Product']:
            return details

        # A) Extract raw text from potential sources
        
        # Source 1: Title
        source_title = raw_title
        
        # Source 2: Highlighted Specs (e.g. "Volumen neto: 650 mL")
        source_highlighted = None
        highlighted_items = soup.find_all('li', class_='ui-vpp-highlighted-specs__features-list-item')
        for item in highlighted_items:
            text = self._safe_get_text(item)
            if 'volumen' in text.lower() or 'neto' in text.lower() or 'unidades' in text.lower():
                source_highlighted = text
                break
        
        # Source 3: Table Specs (Volume, Units, AND Company)
        source_table_volume = None
        source_table_units = None
        source_table_brand = None
        
        rows = soup.find_all('tr', class_='andes-table__row')
        for row in rows:
            header = row.find('th')
            cell = row.find('td')
            if header and cell:
                header_text = self._safe_get_text(header).lower()
                cell_text = self._safe_get_text(cell)
                if 'volumen' in header_text:
                    source_table_volume = cell_text
                elif 'unidades' in header_text or 'formato' in header_text:
                    source_table_units = cell_text
                elif 'marca' in header_text:
                    source_table_brand = cell_text
        
        if source_table_brand:
            details['Company'] = source_table_brand

        # B) Logic based on Search Mode (Units vs Volume)
        if search_mode == 'units':
            # Priority: AI (if wipes) -> Title -> Table Units -> Highlighted
            
            # Special handling for Wipes/Toallitas using AI (as requested to match Amazon logic)
            is_wipes = 'wipes' in keyword.lower() or 'toallitas' in keyword.lower() or 'trapos' in keyword.lower()
            
            if is_wipes:
                self._log("      [MercadoLibre Scraper] Wipes/Rags detected. Using AI for unit count...")
                # We typically use the title for AI extraction as it contains the most context (pack x count)
                ai_units = self.relevance_agent.extract_wipes_units(source_title)
                
                if ai_units > 0:
                    details['Total quantity'] = ai_units
                    details['Unit of measurement'] = 'units'
                    details['Validation_Status'] = 'AI Wipes Units'
                else:
                    # Fallback if AI fails or returns 0
                    p_title = parse_count_string(source_title)
                    if not p_title:
                        p_title = parse_count_string(source_table_units)
                    
                    if p_title:
                        details['Total quantity'] = p_title['quantity']
                        details['Unit of measurement'] = p_title['unit']
                        details['Validation_Status'] = 'Fallback: Title/Table'
            
            else:
                # Standard unit extraction for non-wipes (e.g. brushes, sponges)
                p_title = parse_count_string(source_title)
                if not p_title:
                     p_title = parse_count_string(source_table_units)
                
                if p_title:
                    details['Total quantity'] = p_title['quantity']
                    details['Unit of measurement'] = p_title['unit']
                    details['Validation_Status'] = 'From Title/Table (Units)'

            # If units were found (by AI or Standard), log it
            if details['Total quantity'] > 0:
                 self._log(f"        -> ✨ Units Found: {details['Total quantity']} {details['Unit of measurement']} ({details['Validation_Status']})")

        elif search_mode == 'volume':
            # Robust verification: Need 2 sources to match
            
            # Parse all potential sources
            p_title = parse_volume_string(source_title)
            p_highlighted = parse_volume_string(source_highlighted)
            p_table = parse_volume_string(source_table_volume)
            
            # Sometimes 'Content' is in the table but not 'Volume'
            if not p_table:
                p_table = parse_volume_string(source_table_units)

            volume_sources = {
                'title': p_title,
                'highlighted': p_highlighted,
                'table': p_table
            }
            
            # Filter valid (non-None) sources
            valid_sources = {k: v for k, v in volume_sources.items() if v}
            
            final_data = None
            validation_msg = ""

            # Check for matches
            if len(valid_sources) >= 2:
                source_keys = list(valid_sources.keys())
                for i in range(len(source_keys)):
                    for j in range(i + 1, len(source_keys)):
                        key1, key2 = source_keys[i], source_keys[j]
                        val1, val2 = valid_sources[key1], valid_sources[key2]
                        
                        # Compare normalized values (allow 1% margin or absolute difference < 1)
                        if abs(val1['normalized'] - val2['normalized']) < 1:
                            final_data = val1
                            validation_msg = f"Confirmed by {key1} & {key2}"
                            break
                    if final_data:
                        break
            elif len(valid_sources) == 1:
                # If we only have 1 source matching, we strictly discard as per instructions
                pass 
            
            if final_data:
                details['Total quantity'] = final_data['quantity']
                details['Unit of measurement'] = final_data['unit']
                details['Validation_Status'] = validation_msg
                self._log(f"        -> ✨ Volume Verified: {final_data['quantity']} {final_data['unit']} ({validation_msg})")
            else:
                self._log(f"        -> ⚠️ Volume mismatch or insufficient sources. Sources found: {len(valid_sources)}")
                if len(valid_sources) > 0:
                    self._log(f"           Candidates: {valid_sources}")

        return details

    def _create_driver(self):
        options = uc.ChromeOptions()
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        # options.add_argument('--headless') 

        # Force Chrome version 144 to resolve SessionNotCreatedException
        try:
            return uc.Chrome(options=options, version_main=144)
        except Exception as e:
            time.sleep(5)
            # Retry with explicit version
            return uc.Chrome(options=options, version_main=144)

    def scrape(self, keyword, search_mode):
        self._log(f"   [MercadoLibre Scraper] Searching: '{keyword}' (Mode: {search_mode})")
        found_products = []
        products_to_find = 40 
        
        search_query = keyword.replace(' ', '-')
        search_url = f"https://listado.mercadolibre.com.mx/{search_query}"

        driver = self._create_driver()

        try:
            driver.get(search_url)
            time.sleep(random.uniform(3, 5))
            
            # Reset driver to start URL is done before loop.
            current_search_page_url = search_url
            
            while len(found_products) < products_to_find:
                try:
                    driver.get(current_search_page_url)
                except Exception:
                     # If navigation to search page fails, try restart
                    logger.warning("   ! Navigation to search page failed. Restarting driver...")
                    try: driver.quit() 
                    except: pass
                    driver = self._create_driver()
                    driver.get(current_search_page_url)
                
                time.sleep(random.uniform(3, 6))
                
                try:
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "poly-component__title")))
                except:
                    logger.warning("   ! Results container not found.")
                    break
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                product_links = soup.find_all('a', class_='poly-component__title')
                
                page_product_urls = []
                for link_tag in product_links:
                    if link_tag and link_tag.get('href'):
                        page_product_urls.append(link_tag['href'])
                
                # Visit products found on this page
                for p_url in page_product_urls:
                    if len(found_products) >= products_to_find:
                        break
                        
                    self._log(f"      > Visiting: {p_url[:50]}...")
                    
                    # Retry logic for individual product visit
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            # Check specifically before getting if we prefer, but simplest is inside
                            driver.get(p_url)
                            
                            # Check for Login Wall / Anti-Bot
                            if "ingresa" in driver.title.lower() or "ingresa a tu cuenta" in driver.page_source:
                                logger.warning("      ⚠️ Login/Anti-bot wall detected! Cooling down 20s and restarting with new Identity...")
                                try: driver.quit()
                                except: pass
                                time.sleep(20) # Cooldown to clear temporary block flag
                                driver = self._create_driver()
                                continue # Retry loop which will reload P_URL with new driver
                                
                            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "ui-pdp-title")))
                            p_soup = BeautifulSoup(driver.page_source, 'html.parser')
                            details = self._extract_details_from_product_page(p_soup, search_mode, keyword)
                            details['URL'] = p_url
                            
                            product_title = details.get('Product')
                            if not product_title:
                                self._log(f"        -> Failed to extract product title.")
                                break # Stop retrying this product, it loaded but offered no data

                            if details.get('Total quantity', 0) > 0:
                                self._log(f"        -> 🤖 Asking AI about relevance for: '{product_title[:40]}...'")
                                is_relevant = self.relevance_agent.is_relevant(product_title, keyword)
                                time.sleep(1) 

                                if is_relevant:
                                    found_products.append(details)
                                    self._log(f"        -> ✅ RELEVANT & VALID. Saved: {product_title[:50]}... (${details['Price_SAR']})")
                                else:
                                    self._log(f"        -> ❌ DISCARDED (Not relevant by AI).")
                            else:
                                self._log(f"        -> ⚠️ DISCARDED (No quantity found): {product_title[:60]}...")
                                
                            time.sleep(1)
                            break # Success, exit retry loop
                            
                        except Exception as e:
                            logger.warning(f"      ! Error (Attempt {attempt+1}/{max_retries}): {e}")
                            # If it's a connection error, definitely restart
                            if "WinError" in str(e) or "Connection" in str(e):
                                logger.warning("      ! Connection lost. Restarting driver...")
                                try: driver.quit()
                                except: pass
                                driver = self._create_driver()
                            else:
                                # Standard timeout or other error
                                time.sleep(2)
                    
                    if len(found_products) >= products_to_find:
                        break

                # Go back to search page to find Next button
                driver.get(current_search_page_url)
                time.sleep(3)
                
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, "li.andes-pagination__button--next a.andes-pagination__link")
                    if next_button:
                        current_search_page_url = next_button.get_attribute('href')
                        logger.info("   [MercadoLibre Scraper] Moving to next page...")
                    else:
                        break
                except:
                    logger.info("   [MercadoLibre Scraper] No next page found.")
                    break
                
        except Exception as e:
            logger.error(f"      ! Unexpected error occurred in MercadoLibre scraper", exc_info=True)
        finally:
            if driver:
                driver.quit()

        return found_products
