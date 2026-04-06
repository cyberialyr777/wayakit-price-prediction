import pandas as pd
import time
import csv
import os  
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
     sys.path.insert(0, project_root)
import re
import config
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from services.ai_service import RelevanceAgent
from scrapers.amazon_scraper import AmazonScraper
from scrapers.mercadolibre_scraper import MercadoLibreScraper
from scrapers.petco_scraper import PetcoScraper
from scrapers.walmart_scraper import WalmartScraper
from scrapers.soriana_scraper import SorianaScraper
from scrapers.chedraui_scraper import ChedrauiScraper
from log_config import get_logger

logger = get_logger()

# --- Argumentos ---
parser = argparse.ArgumentParser(description="Ejecuta el scraping de precios de competidores.")
parser.add_argument('--analysis_file', default=config.INSTRUCTIONS_FILE,
                    help=f"Archivo CSV con instrucciones de scraping (default: {config.INSTRUCTIONS_FILE})")
parser.add_argument('--output_mode', default='overwrite', choices=['overwrite', 'append'],
                    help="Modo de escritura para el archivo de salida ('overwrite' o 'append', default: overwrite)")
parser.add_argument('--output_file', default=config.OUTPUT_CSV_FILE,
                    help=f"Archivo CSV de salida para los resultados (default: {config.OUTPUT_CSV_FILE})")
args = parser.parse_args()
# --- FIN Argumentos ---

# --- Renombrar variables de argumentos y config ---
INPUT_ANALYSIS_FILE = args.analysis_file
OUTPUT_MODE = args.output_mode
OUTPUT_SCRAPING_FILE = args.output_file
# --- FIN Renombrar ---

def main():
    logger.info(f"Iniciando scraping. Modo de salida: {OUTPUT_MODE}")
    logger.info(f"Usando archivo de instrucciones: {INPUT_ANALYSIS_FILE}")
    logger.info(f"Archivo de salida: {OUTPUT_SCRAPING_FILE}")

    try:
        # Usar el archivo de análisis pasado como argumento
        df_instructions = pd.read_csv(INPUT_ANALYSIS_FILE)
        df_instructions = df_instructions.dropna(subset=['Type of product', 'Sub industry'])
        logger.info(f"Instrucciones cargadas desde '{INPUT_ANALYSIS_FILE}'. {len(df_instructions)} tareas encontradas.")
    except FileNotFoundError:
        logger.error(f"Archivo de instrucciones '{INPUT_ANALYSIS_FILE}' no encontrado.")
        return
    except Exception as e:
        logger.error(f"Error leyendo '{INPUT_ANALYSIS_FILE}'.", exc_info=True)
        return

    # --- Determinar modo de escritura y cabecera ---
    file_exists = os.path.exists(OUTPUT_SCRAPING_FILE)
    write_mode = 'w' if OUTPUT_MODE == 'overwrite' else 'a'
    write_header = (OUTPUT_MODE == 'overwrite') or (not file_exists)

    # Usar 'with open' fuera del bucle para mejor manejo del archivo
    try:
        with open(OUTPUT_SCRAPING_FILE, write_mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS)
            if write_header:
                writer.writeheader()
                logger.info(f"Escribiendo cabecera en '{OUTPUT_SCRAPING_FILE}'.")

            logger.info("Descargando y configurando ChromeDriver...")
            try:
                driver_path = ChromeDriverManager().install()
                logger.info(f"ChromeDriver listo en: {driver_path}")
            except Exception as e:
                logger.critical("No se pudo instalar ChromeDriver.", exc_info=True)
                return

            ai_agent = RelevanceAgent()

            # Agrupar por subindustria para procesar como lo hacías
            for sub_industry_group, df_industry_instructions in df_instructions.groupby('Sub industry'):
                logger.info(f"\n=================================================")
                logger.info(f"  PROCESANDO SUBINDUSTRIA: '{sub_industry_group}'")
                logger.info(f"=================================================\n")

                if df_industry_instructions.empty:
                    logger.warning(f"No hay instrucciones para '{sub_industry_group}'.")
                    continue

                # Crear scrapers
                scrapers = {
                    'amazon': AmazonScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'mercadolibre': MercadoLibreScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'petco': PetcoScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'walmart': WalmartScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'soriana': SorianaScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'chedraui': ChedrauiScraper(driver_path=driver_path, relevance_agent=ai_agent),
                }

                all_found_products_for_group = []

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
                                site = site.strip().lower()
                                keyword = keyword.strip()
                                if site not in site_specific_keywords:
                                    site_specific_keywords[site] = []
                                site_specific_keywords[site].append(keyword)
                            elif mod:
                                general_modifiers.append(mod)

                    general_modifiers_text = " ".join(general_modifiers)
                    search_keyword = f"{base_keyword} {general_modifiers_text}".strip()

                    search_mode = 'units' if any(kw in original_type_of_product_lower for kw in ['wipes', 'rags', 'microfiber', 'brush']) else 'volume'

                    logger.info(f">> Buscando '{base_keyword}' para '{sub_industry}' (Modo: {search_mode})")
                    
                    sites_to_scrape = config.TARGET_MAP.get(sub_industry, []).copy()

                    for site_name in sites_to_scrape:
                        # --- Exclusión de Chedraui ---
                        if site_name == 'chedraui' and original_type_of_product in config.CHEDRAUI_EXCLUSIONS:
                            logger.info(f"   -> Saltando '{site_name}' para '{original_type_of_product}' (excluido en CHEDRAUI_EXCLUSIONS).")
                            continue

                        scraper = scrapers.get(site_name)
                        if scraper:
                            # Logic for sites that require specific keyword handling (e.g. Petco)
                            keywords_to_use = []
                            if site_name == 'petco':
                                if site_name in site_specific_keywords:
                                    keywords_to_use = site_specific_keywords[site_name]
                                elif general_modifiers:
                                    # Use general modifiers ONLY (ignoring product type name)
                                    keywords_to_use = [" ".join(general_modifiers)]
                                else:
                                    logger.warning(f"Saltando '{site_name}' para '{base_keyword}' porque no se proveyó keyword específica o modificador general.")
                                    continue
                            elif site_name in ('soriana', 'chedraui'):
                                if site_name in site_specific_keywords:
                                    keywords_to_use = site_specific_keywords[site_name]
                                else:
                                    # Fallback to base_keyword (Type of Product ONLY, ignoring general modifiers)
                                    keywords_to_use = [base_keyword]
                            else:
                                # Default logic for Amazon, MercadoLibre
                                keywords_to_use = site_specific_keywords.get(site_name, [search_keyword])

                            for keyword_to_use in keywords_to_use:
                                logger.info(f"   -> Buscando en '{site_name}' con keyword: '{keyword_to_use}'")
                                try:
                                    # Pass max_price filter for Amazon to exclude expensive imports
                                    if site_name == 'amazon':
                                        found_products = scraper.scrape(keyword_to_use, search_mode, max_price=config.AMAZON_MAX_PRICE)
                                    else:
                                        found_products = scraper.scrape(keyword_to_use, search_mode)
                                except Exception as scrape_error:
                                     logger.error(f"Error durante scraping de '{site_name}' con keyword '{keyword_to_use}'.", exc_info=True)
                                     found_products = []

                                b2c_subindustries = ['home', 'automotive', 'pets']
                                channel_value = 'B2C' if sub_industry.lower() in b2c_subindustries else 'B2B'

                                for product in found_products:
                                    row_data = {
                                        'date': time.strftime("%Y-%m-%d"),
                                        'industry': df_industry_instructions.iloc[0]['Industry'],
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
                                    all_found_products_for_group.append(row_data)
                                    logger.debug(f"    -> Producto añadido: {product.get('Product', 'N/A')[:60]}...")
                        else:
                            logger.warning(f"   -> No se encontró scraper para '{site_name}'.")

                if all_found_products_for_group:
                    logger.info(f"Escribiendo {len(all_found_products_for_group)} productos encontrados para '{sub_industry_group}' en '{OUTPUT_SCRAPING_FILE}'...")
                    writer.writerows(all_found_products_for_group)
                else:
                    logger.info(f"No se encontraron productos para guardar en '{sub_industry_group}'.")

                logger.info(f"Proceso para '{sub_industry_group}' completado.")
                logger.info("Descansando 5 segundos antes de la siguiente subindustria...")
                time.sleep(5)

    except IOError as e:
        logger.critical(f"Error fatal al abrir/escribir en '{OUTPUT_SCRAPING_FILE}'.", exc_info=True)
    except Exception as e:
        logger.critical("Error inesperado en el proceso principal de scraping.", exc_info=True)

    logger.info("\n\n--- PROCESO DE SCRAPING COMPLETADO ---")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] Unhandled exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)