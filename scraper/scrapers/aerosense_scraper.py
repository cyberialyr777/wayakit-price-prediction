from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
import time
from utils import extract_aerosense_units 
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

class AeroSenseScraper:
    def __init__(self, driver_path):
        self.driver_path = driver_path
        self.base_url = "https://www.aero-sense.com/en/online-shop/cabin-and-exterior-cleaning"
    
    def _log(self, msg):
        print(msg, flush=True)

    def _parse_package_info(self, package_text):
        volume_ml = 0
        base_match = re.search(r'(\d+[\.,]?\d*)\s*(ml|l)', package_text, re.IGNORECASE)
        if base_match:
            base_volume = float(base_match.group(1).replace(',', '.'))
            unit = base_match.group(2).lower()
            if unit == 'l':
                base_volume *= 1000 
            
            volume_ml = base_volume

            multiplier_match = re.search(r'x\s*(\d+)', package_text, re.IGNORECASE)
            if multiplier_match:
                multiplier = int(multiplier_match.group(1))
                volume_ml *= multiplier
        
        return volume_ml

    def scrape(self, search_term, mode='volume'):
        product_slug = search_term.replace(' ', '-').lower()
        product_url = f"{self.base_url}/{product_slug}"
        
        self._log(f"Navigating directly to AeroSense product page: {product_url}")
        
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
        
        products_found = []
        
        try:
            driver.get(product_url)
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1 div.field--name-title"))
            )
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            product_name_tag = soup.select_one('h1 div.field--name-title')
            product_name = product_name_tag.text.strip() if product_name_tag else "Unknown Product"

            variation_tags = soup.select('#edit-purchased-entity-0-attributes-attribute-volume .js-form-item')

            if not variation_tags:
                self._log(f"No variations found for {product_name}")
                return []

            for variation in variation_tags:
                package_tag = variation.select_one('.package')
                price_tag = variation.select_one('.variationprice')

                if not package_tag or not price_tag:
                    continue

                package_info = package_tag.text.strip()
                price_text = price_tag.text.strip().replace('€', '').replace(',', '').strip()
                price_eur = float(price_text)
                price_sar = round(price_eur * 4.39, 2)

                total_quantity = None
                unit_of_measurement = None

                if mode == 'units':
                    total_quantity = extract_aerosense_units(package_info)
                    unit_of_measurement = 'units'
                    self._log(f"  - Found variation: {product_name} | {package_info} | Price: {price_sar:.2f} SAR | Units: {total_quantity}")
                else:
                    total_quantity = self._parse_package_info(package_info)
                    unit_of_measurement = 'ml'
                    self._log(f"  - Found variation: {product_name} | {package_info} | Price: {price_sar:.2f} SAR | Volume: {total_quantity}ml")


                product_data = {
                    'Company': 'AeroSense',
                    'Product': f"{product_name} - {package_info}",
                    'Price_SAR': price_sar,
                    'Total quantity': total_quantity,
                    'Unit of measurement': unit_of_measurement,
                    'URL': product_url
                }
                products_found.append(product_data)

        except Exception as e:
            print(f"An error occurred while scraping {product_url}: {e}")
        finally:
            if driver:
                driver.quit()
        
        return products_found

