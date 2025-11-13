from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
from utils import parse_volume_string, parse_count_string
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from log_config import get_logger

logger = get_logger()

class AmazonScraper:
    def __init__(self, driver_path, relevance_agent):
        self.driver_path = driver_path
        self.relevance_agent = relevance_agent 
        self.base_url = "https://www.amazon.sa"

    def _log(self, msg):
        logger.info(msg)

    def _safe_get_text(self, element):
        return element.get_text(strip=True) if element else None

    def _extract_from_table(self, soup, table_id, fields):
        results = {field: None for field in fields}
        table = soup.find('table', id=table_id)
        if table:
            for row in table.find_all('tr'):
                header = row.find('th')
                value_cell = row.find('td')
                if header and value_cell:
                    header_text = header.get_text(strip=True).lower()
                    for field in fields:
                        if field in header_text:
                            results[field] = value_cell.get_text(strip=True)
        return results

    
    def _extract_details_from_product_page(self, soup, search_mode, keyword):
        details = {
            'Product': None, 'Price_SAR': '0.00', 'Company': 'Company not found',
            'Unit of measurement': 'units', 'Total quantity': 0, 'Validation_Status': 'Not Found'
        }

        details['Product'] = self._safe_get_text(soup.find('span', id='productTitle'))
        brand_row = soup.find('tr', class_='po-brand')
        details['Company'] = self._safe_get_text(brand_row.find('span', class_='po-break-word')) if brand_row else details['Company']
        price_whole = self._safe_get_text(soup.find('span', class_='a-price-whole'))
        price_fraction = self._safe_get_text(soup.find('span', class_='a-price-fraction'))
        
        if price_whole:
            price_str = price_whole.replace(',', '').rstrip('.')
            details['Price_SAR'] = f"{price_str}.{price_fraction}" if price_fraction else price_str

        raw_title = details.get('Product')
        if not raw_title:
            return details 
        if search_mode == 'units':
            self._log("      [Extractor] Mode: Units")
            kw = keyword.lower()
            is_wipes_or_rags = ('wipes' in kw) or ('rags' in kw)

            if is_wipes_or_rags:
                logger.debug("      -> Wipes/Rags detected. Using AI for unit count...")
                ai_units = self.relevance_agent.extract_wipes_units(raw_title)
                if ai_units > 0:
                    details['Total quantity'] = ai_units
                    details['Validation_Status'] = 'AI Wipes Units'
            else:
                logger.debug("      -> Other units detected. Using local parser...")
                p_title = parse_count_string(raw_title)
                if p_title:
                    details['Total quantity'] = p_title['quantity']
                    details['Unit of measurement'] = p_title['unit']
                    details['Validation_Status'] = 'From Title'
        
        elif search_mode == 'volume':
            self._log("      [Extractor] Mode: Volume")
            tech_fields = self._extract_from_table(soup, 'productDetails_techSpec_section_1', ['volume', 'weight'])
            item_volume_row = soup.find('tr', class_='po-item_volume')
            raw_item_volume = self._safe_get_text(item_volume_row.find('span', class_='po-break-word')) if item_volume_row else None
            logger.debug(f"      [Debug] -> Vol: '{tech_fields['volume']}', ItemVol: '{raw_item_volume}'")

            p_title = parse_volume_string(raw_title)
            p_volume = parse_volume_string(tech_fields['volume'])
            p_item_volume = parse_volume_string(raw_item_volume)
            
            volume_sources = {'title': p_title, 'volume': p_volume, 'item_volume': p_item_volume}
            valid_sources = {k: v for k, v in volume_sources.items() if v}

            if len(valid_sources) >= 2:
                source_keys = list(valid_sources.keys())
                final_data = None
                for i in range(len(source_keys)):
                    for j in range(i + 1, len(source_keys)):
                        key1, key2 = source_keys[i], source_keys[j]
                        val1, val2 = valid_sources[key1], valid_sources[key2]
                        if abs(val1['normalized'] - val2['normalized']) < 1:
                            final_data = val1
                            validation_status = f"Confirmed by {key1.capitalize()} & {key2.capitalize()}"
                            details['Total quantity'] = final_data['quantity']
                            details['Unit of measurement'] = final_data['unit']
                            details['Validation_Status'] = validation_status
                            break
                    if final_data:
                        break
        
        if details.get('Total quantity', 0) > 0:
            logger.debug(f"      [Extractor] ✅ Quantity found: {details['Total quantity']} {details['Unit of measurement']} (Source: {details['Validation_Status']})")
        else:
            logger.debug("      [Extractor] ❌ No valid quantity found on page.")
            
        return details
    
    def scrape(self, keyword, search_mode):
        self._log(f"   [Amazon Scraper] Searching: '{keyword}' (Mode: {search_mode})")
        found_products = []
        products_to_find = 40
        search_url = f"{self.base_url}/s?k={keyword.replace(' ', '+')}&language=en_AE"

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
            driver.get(search_url)
            import time; time.sleep(3)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-component-type='s-search-result']")))
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            product_containers = soup.find_all('div', {'data-component-type': 's-search-result'})

            for container in product_containers:
                if len(found_products) >= products_to_find:
                    break

                link_tag = container.find('a', class_='a-link-normal')
                if not link_tag or 'spons' in link_tag.get('href', ''):
                    continue
                product_url = urljoin(self.base_url, link_tag['href'])
                self._log(f"      > Visiting product page: {product_url[:120]}...")
                driver.get(product_url)
                try:
                    WebDriverWait(driver, 10).until(EC.any_of(
                        EC.presence_of_element_located((By.ID, "productDetails_techSpec_section_1")),
                        EC.presence_of_element_located((By.ID, "detailBullets_feature_div")),
                        EC.presence_of_element_located((By.CLASS_NAME, "po-item_volume"))
                    ))
                except Exception:
                    logger.warning("      ! Details section not found, skipping.")
                    continue
                
                product_soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                product_details = self._extract_details_from_product_page(product_soup, search_mode, keyword)
                product_details['URL'] = product_url
                product_title = product_details.get('Product')

                if not product_title:
                    logger.warning(f"      -> DISCARDED (No title found)")
                    continue

                if product_details.get('Total quantity', 0) > 0:
                    is_relevant = self.relevance_agent.is_relevant(product_title, keyword)
                    time.sleep(2)

                    if is_relevant:
                        found_products.append(product_details)
                        logger.info(f"      -> ✅ RELEVANT & VALID. Product saved.")
                    else:
                        logger.info(f"      -> DISCARDED (Not relevant by AI): {product_title[:60]}...")
                else:
                    logger.info(f"      -> DISCARDED (No quantity found by extractor): {product_title[:60]}...")
                
        except Exception as e:
            logger.error(f"      ! Unexpected error occurred in Amazon scraper", exc_info=True)
        finally:
            if driver:
                driver.quit()

        return found_products