import pandas as pd
import time
import csv
import os  
import re
import sys
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from services.ai_service import RelevanceAgent
from scrapers.amazon_scraper import AmazonScraper
from scrapers.mumzworld_scraper import MumzworldScraper
from scrapers.saco_scraper import SacoScraper
from scrapers.fine_scraper import FineScraper
from scrapers.gogreen_scraper import GoGreenScraper 
from scrapers.officesupply_scraper import OfficeSupplyScraper
from scrapers.aerosense_scraper import AeroSenseScraper
from log_config import get_logger

logger = get_logger()

def main():
    try:
        df_instructions = pd.read_csv(config.INSTRUCTIONS_FILE)
        df_instructions = df_instructions.dropna(subset=['Type of product', 'Sub industry'])
    except FileNotFoundError:
        logger.error(f"Error: Instructions file '{config.INSTRUCTIONS_FILE}' was not found.")
        return

    write_header = not os.path.exists(config.OUTPUT_CSV_FILE)
    
    with open(config.OUTPUT_CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS)
        if write_header:
            writer.writeheader()

    logger.info("Downloading and configuring ChromeDriver once...")
    try:
        driver_path = ChromeDriverManager().install()
        logger.info(f"ChromeDriver installed at: {driver_path}")
    except Exception as e:
        logger.error(f"Fatal error: Could not install ChromeDriver.", exc_info=True)
        return

    for industry_to_scrape in config.TARGET_MAP.keys():
        logger.info(f"\n=================================================")
        logger.info(f"  INICIANDO PROCESO PARA LA SUBINDUSTRIA: '{industry_to_scrape}'")
        logger.info(f"=================================================\n")

        df_industry_instructions = df_instructions[df_instructions['Sub industry'] == industry_to_scrape].copy()

        if df_industry_instructions.empty:
            logger.warning(f"  -> No products found for subindustry '{industry_to_scrape}'. Skipping to next.")
            continue

        ai_agent = RelevanceAgent()
        scrapers = {
            'amazon': AmazonScraper(driver_path=driver_path, relevance_agent=ai_agent),
            'mumzworld': MumzworldScraper(driver_path=driver_path, relevance_agent=ai_agent),
            'saco': SacoScraper(driver_path=driver_path, relevance_agent=ai_agent),
            'fine': FineScraper(driver_path=driver_path, relevance_agent=ai_agent),
            'gogreen': GoGreenScraper(driver_path=driver_path, relevance_agent=ai_agent),
            'officesupply': OfficeSupplyScraper(driver_path=driver_path, relevance_agent=ai_agent),
            'aerosense': AeroSenseScraper(driver_path=driver_path)
        }
        
        all_found_products = []

        for index, row in df_industry_instructions.iterrows():
            sub_industry = row['Sub industry']
            original_type_of_product = str(row['Type of product'])
            original_type_of_product_lower = original_type_of_product.lower() 
            generic_type_of_product = str(row['Generic product type'])
            
            try:
                base_keyword = original_type_of_product_lower.split('-', 1)[1].strip()
            except IndexError:
                base_keyword = original_type_of_product_lower.strip()

            search_modifiers_val = row.get('Search Modifiers')
            site_specific_keywords = {}
            general_modifiers = []

            if pd.notna(search_modifiers_val):
                search_modifiers_str = str(search_modifiers_val)
                modifiers = search_modifiers_str.split(';')
                for mod in modifiers:
                    mod = mod.strip()
                    if ':' in mod:
                        site, keyword = mod.split(':', 1)
                        site = site.strip()
                        keyword = keyword.strip()
                        if site not in site_specific_keywords:
                            site_specific_keywords[site] = []
                        site_specific_keywords[site].append(keyword)
                    elif mod:
                        general_modifiers.append(mod)
            
            general_modifiers_text = " ".join(general_modifiers)
            search_keyword = f"{base_keyword} {general_modifiers_text}".strip()
            
            search_mode = 'units' if any(keyword in original_type_of_product_lower for keyword in ['wipes', 'rags', 'microfiber', 'brush']) else 'volume'

            logger.info(f">> Buscando '{base_keyword}' para '{sub_industry}' (Modo: {search_mode})")
            for site, kword in site_specific_keywords.items():
                logger.debug(f"   -> {site.capitalize()} usará el término específico: '{kword}'")

            sites_to_scrape = config.TARGET_MAP.get(sub_industry, []).copy()
            
            if base_keyword in config.MUMZWORLD_EXCLUSIONS and 'mumzworld' in sites_to_scrape:
                sites_to_scrape.remove('mumzworld')
            
            if base_keyword in config.SACO_EXCLUSIONS and 'saco' in sites_to_scrape:
                sites_to_scrape.remove('saco')

            for site_name in sites_to_scrape:
                scraper = scrapers.get(site_name)
                if scraper:
                    keywords_to_use = site_specific_keywords.get(site_name, [search_keyword])

                    for keyword_to_use in keywords_to_use:
                        if site_name in ['fine', 'gogreen', 'officesupply', 'aerosense']:
                             if site_name not in site_specific_keywords:
                                logger.debug(f"   -> Saltando '{site_name}' porque no se proveyó una etiqueta específica (ej. '{site_name}:...')")
                                continue
                        
                        logger.info(f"   -> Buscando en '{site_name}' con la palabra clave: '{keyword_to_use}'")
                        found_products = scraper.scrape(keyword_to_use, search_mode)
                        
                        b2c_subindustries = ['home', 'automotive', 'pets']
                        channel_value = 'B2C' if sub_industry.lower() in b2c_subindustries else 'B2B'
                        
                        for product in found_products:
                            row_data = {
                                'date': time.strftime("%Y-%m-%d"),
                                'industry': industry_to_scrape,
                                'subindustry': sub_industry,
                                'type_of_product': original_type_of_product,
                                'generic_product_type': generic_type_of_product,
                                'product': product.get('Product'),
                                'price_sar': product.get('Price_SAR'),
                                'company': product.get('Company'),
                                'source': site_name,
                                'url': product.get('URL'),
                                'unit_of_measurement': product.get('Unit of measurement'),
                                'total_quantity': product.get('Total quantity'),
                                'channel': channel_value,
                            }
                            all_found_products.append(row_data)
                            logger.info(f"    -> SAVED: {product.get('Product', 'N/A')[:60]}... (Source: {site_name})")
                else:
                    logger.warning(f"   -> Warning: Scraper not found for site '{site_name}'.")

        if all_found_products:
            logger.info(f"\n--- Saving {len(all_found_products)} products found for industry '{industry_to_scrape}' ---")
            try:
                with open(config.OUTPUT_CSV_FILE, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS)
                    writer.writerows(all_found_products)
                logger.info(f"  -> Success! Data added to '{config.OUTPUT_CSV_FILE}'.")
            except IOError as e:
                logger.error(f"  -> Error writing to CSV file", exc_info=True)
        else:
            logger.info(f"\n--- No products found to save for industry '{industry_to_scrape}'. ---")

        logger.info(f"\n  -> Process for '{industry_to_scrape}' completed.")
        logger.info("  -> Resting 10 seconds before next industry...")
        time.sleep(10)

    logger.info("\n\n--- COMPLETE PROCESS FOR ALL INDUSTRIES ---")

if __name__ == "__main__":
    try:
        try:
            sys.stdout.reconfigure(line_buffering=True, write_through=True)
        except Exception:
            pass
        main()
    except Exception as e:
        import traceback
        logger.error(f"[FATAL] Unhandled exception", exc_info=True)
        traceback.print_exc()