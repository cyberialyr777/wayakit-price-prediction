from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
import time
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from log_config import get_logger
from utils import separate_title_and_volume, separate_title_and_units

logger = get_logger()

class PetcoScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent
        self.base_url = "https://petco.com.mx"

    def _log(self, msg):
        logger.info(msg)

    def _safe_get_text(self, element):
        return element.get_text(strip=True) if element else None

    def scrape(self, keyword, search_mode):
        self._log(f"   [Petco Scraper] Searching: '{keyword}' (Mode: {search_mode})")
        found_products = []
        products_to_find = 20
        
        # Build Search URL
        search_url = f"{self.base_url}/petco/en/search/?text={keyword.replace(' ', '+')}"

        service = ChromeService(executable_path=self.driver_path)
        options = webdriver.ChromeOptions()
        # Copy options from Amazon scraper regarding headers/automation flags
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
        
        driver = webdriver.Chrome(service=service, options=options)
        
        try:
            driver.get(search_url)
            time.sleep(3)
            
            while len(found_products) < products_to_find:
                # Check and close popup if exists
                try:
                    close_btn = driver.find_elements(By.ID, "wps-overlay-close-button")
                    if close_btn:
                        self._log("      > Popup detected. Closing...")
                        close_btn[0].click()
                        time.sleep(1)
                except Exception:
                    pass



                # Wait for results to ensure page loaded
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "product-item")) 
                    )
                except:
                     pass # Proceed to parse what we can

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Optimization: Stop if we hit the "spelling suggestion" block (irrelevant results follow)
                # We look for BOTH class 'details' (products) and 'searchSpellingSuggestionPromptSearch' (boundary)
                all_elements = soup.find_all('div', class_=['details', 'searchSpellingSuggestionPromptSearch'])
                
                product_divs = []
                stop_search_after_page = False

                for elem in all_elements:
                    classes = elem.get('class', [])
                    if 'searchSpellingSuggestionPromptSearch' in classes:
                        self._log("      ! Spelling suggestion marker reached. Ignoring subsequent results on this page.")
                        stop_search_after_page = True
                        break
                    
                    if 'details' in classes:
                        product_divs.append(elem)
                
                if not product_divs:
                    self._log("      ! No (more) product details found on page.")
                    break

                for detail_div in product_divs:
                    if len(found_products) >= products_to_find:
                        break
                        
                    # 1. Extract Brand
                    brand_span = detail_div.find('span', class_='grig-bran')
                    brand = self._safe_get_text(brand_span) or "Unknown Brand"
                    
                    # 2. Extract Generic Name (Title)
                    name_tag = detail_div.find('a', class_='name')
                    raw_name = self._safe_get_text(name_tag)
                    
                    if not raw_name:
                        continue

                    # 3. Extract Price
                    # 3. Extract Price
                    price_sar = '0.00'
                    
                    # Attempt to find the product container to locate the price element
                    # We look up for 'product-item' wrapper or 'col-xs-12' container
                    product_container = detail_div.find_parent(class_='product-item')
                    if not product_container:
                        product_container = detail_div.find_parent(class_='col-xs-12')
                    
                    if product_container:
                        # User specified "div.price" as the ONLY source of truth
                        # The text is inside: <div class="price ..."> $209.00 </div>
                        price_element = product_container.find('div', class_='price')

                        if price_element:
                            price_text = self._safe_get_text(price_element)
                            if price_text:
                                # Clean price: "$271.15" -> "271.15"
                                # handle cases like "$ 271.15" or " $271.15 "
                                clean_text = price_text.replace('$', '').replace(',', '').strip()
                                # Verify it has digits
                                if any(char.isdigit() for char in clean_text):
                                    price_sar = clean_text

                    # 4. Separate Title -> (Clean Title + Quantity)
                    extracted_data = {}
                    if search_mode == 'units':
                        extracted_data = separate_title_and_units(raw_name)
                    else:
                        extracted_data = separate_title_and_volume(raw_name)
                        
                    final_title = extracted_data['title']
                    quantity_data = extracted_data.get('unit_data') or extracted_data.get('volume_data')
                    
                    # 5. Filter by Relevance (AI)
                    if quantity_data and quantity_data['quantity'] > 0:
                         # Deduplication Check
                         if any(p['Product'] == final_title for p in found_products):
                             self._log(f"      -> Duplicate detected. Skipping: {final_title[:40]}...")
                             continue

                         is_relevant = self.relevance_agent.is_relevant(final_title, keyword)
                         if is_relevant:
                             product_data = {
                                 'Product': final_title,
                                 'Price_SAR': price_sar,
                                 'Company': brand,
                                 'Unit of measurement': quantity_data['unit'],
                                 'Total quantity': quantity_data['quantity'],
                                 'URL': urljoin(self.base_url, name_tag['href']) if name_tag else None
                             }
                             found_products.append(product_data)
                             self._log(f"      -> ✅ Product saved: {final_title[:40]}... ({quantity_data['quantity']} {quantity_data['unit']})")
                         else:
                             self._log(f"      -> DISCARDED (Not relevant): {final_title[:40]}...")
                    else:
                        self._log(f"      -> DISCARDED (No quantity): {final_title[:40]}...")

                # Pagination Logic
                if stop_search_after_page:
                    self._log("      > Stopping pagination due to spelling suggestion marker.")
                    break

                if len(found_products) < products_to_find:
                    # Look for "Next" button
                    try:
                        next_btn_li = driver.find_elements(By.CSS_SELECTOR, "li.pagination-next")
                        if next_btn_li:
                            next_link = next_btn_li[0].find_element(By.TAG_NAME, "a")
                            next_href = next_link.get_attribute('href')
                            
                            if next_href and 'javascript' not in next_href and 'disabled' not in next_btn_li[0].get_attribute('class'):
                                self._log("      > Navigating to next page...")
                                driver.get(next_href)
                                time.sleep(3)
                            else:
                                self._log("      > No more pages (Next button disabled or invalid).")
                                break
                        else:
                            break
                    except Exception:
                        self._log("      ! Error navigating to next page.")
                        break
                else:
                    break

        except Exception as e:
            logger.error(f"      ! Unexpected error in Petco scraper", exc_info=True)
        finally:
            if driver:
                driver.quit()
        
        return found_products
