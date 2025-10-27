# import pandas as pd
# import time
# import csv
# import os  
# import re
# import sys
# import config
# import argparse
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
# from services.ai_service import RelevanceAgent
# from scrapers.amazon_scraper import AmazonScraper
# from scrapers.mumzworld_scraper import MumzworldScraper
# from scrapers.saco_scraper import SacoScraper
# from scrapers.fine_scraper import FineScraper
# from scrapers.gogreen_scraper import GoGreenScraper 
# from scrapers.officesupply_scraper import OfficeSupplyScraper
# from scrapers.aerosense_scraper import AeroSenseScraper
# from log_config import get_logger

# logger = get_logger()

# def main():
#     try:
#         df_instructions = pd.read_csv(config.INSTRUCTIONS_FILE)
#         df_instructions = df_instructions.dropna(subset=['Type of product', 'Sub industry'])
#     except FileNotFoundError:
#         logger.error(f"Error: Instructions file '{config.INSTRUCTIONS_FILE}' was not found.")
#         return

#     write_header = not os.path.exists(config.OUTPUT_CSV_FILE)
    
#     with open(config.OUTPUT_CSV_FILE, 'a', newline='', encoding='utf-8') as f:
#         writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS)
#         if write_header:
#             writer.writeheader()

#     logger.info("Downloading and configuring ChromeDriver once...")
#     try:
#         driver_path = ChromeDriverManager().install()
#         logger.info(f"ChromeDriver installed at: {driver_path}")
#     except Exception as e:
#         logger.error(f"Fatal error: Could not install ChromeDriver.", exc_info=True)
#         return

#     for industry_to_scrape in config.TARGET_MAP.keys():
#         logger.info(f"\n=================================================")
#         logger.info(f"  INICIANDO PROCESO PARA LA SUBINDUSTRIA: '{industry_to_scrape}'")
#         logger.info(f"=================================================\n")

#         df_industry_instructions = df_instructions[df_instructions['Sub industry'] == industry_to_scrape].copy()

#         if df_industry_instructions.empty:
#             logger.warning(f"  -> No products found for subindustry '{industry_to_scrape}'. Skipping to next.")
#             continue

#         ai_agent = RelevanceAgent()
#         scrapers = {
#             'amazon': AmazonScraper(driver_path=driver_path, relevance_agent=ai_agent),
#             'mumzworld': MumzworldScraper(driver_path=driver_path, relevance_agent=ai_agent),
#             'saco': SacoScraper(driver_path=driver_path, relevance_agent=ai_agent),
#             'fine': FineScraper(driver_path=driver_path, relevance_agent=ai_agent),
#             'gogreen': GoGreenScraper(driver_path=driver_path, relevance_agent=ai_agent),
#             'officesupply': OfficeSupplyScraper(driver_path=driver_path, relevance_agent=ai_agent),
#             'aerosense': AeroSenseScraper(driver_path=driver_path)
#         }
        
#         all_found_products = []

#         for index, row in df_industry_instructions.iterrows():
#             sub_industry = row['Sub industry']
#             original_type_of_product = str(row['Type of product'])
#             original_type_of_product_lower = original_type_of_product.lower() 
#             generic_type_of_product = str(row['Generic product type'])
            
#             try:
#                 base_keyword = original_type_of_product_lower.split('-', 1)[1].strip()
#             except IndexError:
#                 base_keyword = original_type_of_product_lower.strip()

#             search_modifiers_val = row.get('Search Modifiers')
#             site_specific_keywords = {}
#             general_modifiers = []

#             if pd.notna(search_modifiers_val):
#                 search_modifiers_str = str(search_modifiers_val)
#                 modifiers = search_modifiers_str.split(';')
#                 for mod in modifiers:
#                     mod = mod.strip()
#                     if ':' in mod:
#                         site, keyword = mod.split(':', 1)
#                         site = site.strip()
#                         keyword = keyword.strip()
#                         if site not in site_specific_keywords:
#                             site_specific_keywords[site] = []
#                         site_specific_keywords[site].append(keyword)
#                     elif mod:
#                         general_modifiers.append(mod)
            
#             general_modifiers_text = " ".join(general_modifiers)
#             search_keyword = f"{base_keyword} {general_modifiers_text}".strip()
            
#             search_mode = 'units' if any(keyword in original_type_of_product_lower for keyword in ['wipes', 'rags', 'microfiber', 'brush']) else 'volume'

#             logger.info(f">> Buscando '{base_keyword}' para '{sub_industry}' (Modo: {search_mode})")
#             for site, kword in site_specific_keywords.items():
#                 logger.debug(f"   -> {site.capitalize()} usará el término específico: '{kword}'")

#             sites_to_scrape = config.TARGET_MAP.get(sub_industry, []).copy()
            
#             if base_keyword in config.MUMZWORLD_EXCLUSIONS and 'mumzworld' in sites_to_scrape:
#                 sites_to_scrape.remove('mumzworld')
            
#             if base_keyword in config.SACO_EXCLUSIONS and 'saco' in sites_to_scrape:
#                 sites_to_scrape.remove('saco')

#             for site_name in sites_to_scrape:
#                 scraper = scrapers.get(site_name)
#                 if scraper:
#                     keywords_to_use = site_specific_keywords.get(site_name, [search_keyword])

#                     for keyword_to_use in keywords_to_use:
#                         if site_name in ['fine', 'gogreen', 'officesupply', 'aerosense']:
#                              if site_name not in site_specific_keywords:
#                                 logger.debug(f"   -> Saltando '{site_name}' porque no se proveyó una etiqueta específica (ej. '{site_name}:...')")
#                                 continue
                        
#                         logger.info(f"   -> Buscando en '{site_name}' con la palabra clave: '{keyword_to_use}'")
#                         found_products = scraper.scrape(keyword_to_use, search_mode)
                        
#                         b2c_subindustries = ['home', 'automotive', 'pets']
#                         channel_value = 'B2C' if sub_industry.lower() in b2c_subindustries else 'B2B'
                        
#                         for product in found_products:
#                             row_data = {
#                                 'date': time.strftime("%Y-%m-%d"),
#                                 'industry': industry_to_scrape,
#                                 'subindustry': sub_industry,
#                                 'type_of_product': original_type_of_product,
#                                 'generic_product_type': generic_type_of_product,
#                                 'product': product.get('Product'),
#                                 'price_sar': product.get('Price_SAR'),
#                                 'company': product.get('Company'),
#                                 'source': site_name,
#                                 'url': product.get('URL'),
#                                 'unit_of_measurement': product.get('Unit of measurement'),
#                                 'total_quantity': product.get('Total quantity'),
#                                 'channel': channel_value,
#                             }
#                             all_found_products.append(row_data)
#                             logger.info(f"    -> SAVED: {product.get('Product', 'N/A')[:60]}... (Source: {site_name})")
#                 else:
#                     logger.warning(f"   -> Warning: Scraper not found for site '{site_name}'.")

#         if all_found_products:
#             logger.info(f"\n--- Saving {len(all_found_products)} products found for industry '{industry_to_scrape}' ---")
#             try:
#                 with open(config.OUTPUT_CSV_FILE, 'a', newline='', encoding='utf-8') as f:
#                     writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS)
#                     writer.writerows(all_found_products)
#                 logger.info(f"  -> Success! Data added to '{config.OUTPUT_CSV_FILE}'.")
#             except IOError as e:
#                 logger.error(f"  -> Error writing to CSV file", exc_info=True)
#         else:
#             logger.info(f"\n--- No products found to save for industry '{industry_to_scrape}'. ---")

#         logger.info(f"\n  -> Process for '{industry_to_scrape}' completed.")
#         logger.info("  -> Resting 10 seconds before next industry...")
#         time.sleep(10)

#     logger.info("\n\n--- COMPLETE PROCESS FOR ALL INDUSTRIES ---")

# if __name__ == "__main__":
#     try:
#         try:
#             sys.stdout.reconfigure(line_buffering=True, write_through=True)
#         except Exception:
#             pass
#         main()
#     except Exception as e:
#         import traceback
#         logger.error(f"[FATAL] Unhandled exception", exc_info=True)
#         traceback.print_exc()

# scraper/main.py (Modificaciones + Argumentos)
import pandas as pd
import time
import csv
import os  
import re
import sys
import config
import argparse
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

logger = get_logger() # <-- AÑADIDO

# --- Argumentos --- AÑADIDO ---
parser = argparse.ArgumentParser(description="Ejecuta el scraping de precios de competidores.")
parser.add_argument('--analysis_file', default=config.INSTRUCTIONS_FILE,
                    help=f"Archivo CSV con instrucciones de scraping (default: {config.INSTRUCTIONS_FILE})")
parser.add_argument('--output_mode', default='overwrite', choices=['overwrite', 'append'],
                    help="Modo de escritura para el archivo de salida ('overwrite' o 'append', default: overwrite)")
parser.add_argument('--output_file', default=config.OUTPUT_CSV_FILE,
                    help=f"Archivo CSV de salida para los resultados (default: {config.OUTPUT_CSV_FILE})")
args = parser.parse_args()
# --- FIN Argumentos ---

# --- Renombrar variables de argumentos y config --- AÑADIDO ---
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

    # --- Determinar modo de escritura y cabecera --- MODIFICADO ---
    file_exists = os.path.exists(OUTPUT_SCRAPING_FILE)
    write_mode = 'w' if OUTPUT_MODE == 'overwrite' else 'a'
    write_header = (OUTPUT_MODE == 'overwrite') or (not file_exists) # Escribir cabecera si se sobrescribe o si el archivo no existe en modo append

    # Usar 'with open' fuera del bucle para mejor manejo del archivo
    try:
        with open(OUTPUT_SCRAPING_FILE, write_mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS)
            if write_header:
                writer.writeheader()
                logger.info(f"Escribiendo cabecera en '{OUTPUT_SCRAPING_FILE}'.")
            # --- FIN Modificación ---

            logger.info("Descargando y configurando ChromeDriver...")
            try:
                driver_path = ChromeDriverManager().install()
                logger.info(f"ChromeDriver listo en: {driver_path}")
            except Exception as e:
                logger.critical("No se pudo instalar ChromeDriver.", exc_info=True)
                return # Salir si no hay driver

            ai_agent = RelevanceAgent() # Asume que RelevanceAgent ya usa Secrets Manager internamente

            # Agrupar por subindustria para procesar como lo hacías
            for sub_industry_group, df_industry_instructions in df_instructions.groupby('Sub industry'):
                logger.info(f"\n=================================================")
                logger.info(f"  PROCESANDO SUBINDUSTRIA: '{sub_industry_group}'")
                logger.info(f"=================================================\n")

                if df_industry_instructions.empty:
                    logger.warning(f"No hay instrucciones para '{sub_industry_group}'.")
                    continue

                # Crear scrapers (puedes mover esto fuera del bucle si prefieres)
                scrapers = {
                    'amazon': AmazonScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'mumzworld': MumzworldScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'saco': SacoScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'fine': FineScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'gogreen': GoGreenScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'officesupply': OfficeSupplyScraper(driver_path=driver_path, relevance_agent=ai_agent),
                    'aerosense': AeroSenseScraper(driver_path=driver_path) # Aerosense no usa AI agent? Verificar
                }

                all_found_products_for_group = [] # Acumular productos por grupo

                for index, row in df_industry_instructions.iterrows():
                    # ... (lógica para extraer base_keyword, modifiers, search_keyword, search_mode - sin cambios) ...
                    sub_industry = row['Sub industry'] # Asegurar que sub_industry se define aquí
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
                                site = site.strip().lower() # asegurar minúsculas
                                keyword = keyword.strip()
                                if site not in site_specific_keywords:
                                    site_specific_keywords[site] = []
                                site_specific_keywords[site].append(keyword)
                            elif mod:
                                general_modifiers.append(mod)

                    general_modifiers_text = " ".join(general_modifiers)
                    search_keyword = f"{base_keyword} {general_modifiers_text}".strip()

                    search_mode = 'units' if any(kw in original_type_of_product_lower for kw in ['wipes', 'rags', 'microfiber', 'brush']) else 'volume'
                    # ... (fin lógica extracción) ...

                    logger.info(f">> Buscando '{base_keyword}' para '{sub_industry}' (Modo: {search_mode})")
                    # ... (lógica de sitios a scrapear, exclusiones - sin cambios) ...
                    sites_to_scrape = config.TARGET_MAP.get(sub_industry, []).copy()

                    if base_keyword in config.MUMZWORLD_EXCLUSIONS and 'mumzworld' in sites_to_scrape:
                        sites_to_scrape.remove('mumzworld')
                        logger.debug(f"Excluyendo Mumzworld para '{base_keyword}'")

                    if base_keyword in config.SACO_EXCLUSIONS and 'saco' in sites_to_scrape:
                        sites_to_scrape.remove('saco')
                        logger.debug(f"Excluyendo Saco para '{base_keyword}'")


                    for site_name in sites_to_scrape:
                        scraper = scrapers.get(site_name)
                        if scraper:
                            # Decidir qué keyword usar: específico del sitio o el general
                            keywords_to_use = site_specific_keywords.get(site_name, [search_keyword])

                            for keyword_to_use in keywords_to_use:
                                # Lógica para sitios B2B que requieren keyword específica
                                if site_name in ['fine', 'gogreen', 'officesupply', 'aerosense']:
                                     if site_name not in site_specific_keywords and not general_modifiers: # Saltar si no hay keyword específica Y TAMPOCO modificadores generales
                                        logger.warning(f"Saltando '{site_name}' para '{base_keyword}' porque no se proveyó keyword específica o modificador general.")
                                        continue

                                logger.info(f"   -> Buscando en '{site_name}' con keyword: '{keyword_to_use}'")
                                try:
                                    found_products = scraper.scrape(keyword_to_use, search_mode)
                                except Exception as scrape_error:
                                     logger.error(f"Error durante scraping de '{site_name}' con keyword '{keyword_to_use}'.", exc_info=True)
                                     found_products = [] # Continuar con el siguiente sitio/keyword

                                b2c_subindustries = ['home', 'automotive', 'pets'] # Usar minúsculas para comparación
                                channel_value = 'B2C' if sub_industry.lower() in b2c_subindustries else 'B2B'

                                for product in found_products:
                                    row_data = {
                                        # ... (mapeo de datos - sin cambios) ...
                                        'date': time.strftime("%Y-%m-%d"),
                                        'industry': df_industry_instructions.iloc[0]['Industry'], # Obtener de la primera fila del grupo
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
                                    logger.debug(f"    -> Producto añadido: {product.get('Product', 'N/A')[:60]}...") # Cambiado a DEBUG para menos verbosidad
                        else:
                            logger.warning(f"   -> No se encontró scraper para '{site_name}'.")

                # Escribir los resultados del grupo actual al archivo CSV
                if all_found_products_for_group:
                    logger.info(f"Escribiendo {len(all_found_products_for_group)} productos encontrados para '{sub_industry_group}' en '{OUTPUT_SCRAPING_FILE}'...")
                    writer.writerows(all_found_products_for_group)
                else:
                    logger.info(f"No se encontraron productos para guardar en '{sub_industry_group}'.")

                logger.info(f"Proceso para '{sub_industry_group}' completado.")
                logger.info("Descansando 5 segundos antes de la siguiente subindustria...") # Reducido tiempo de descanso?
                time.sleep(5)

    except IOError as e:
        logger.critical(f"Error fatal al abrir/escribir en '{OUTPUT_SCRAPING_FILE}'.", exc_info=True)
    except Exception as e:
        logger.critical("Error inesperado en el proceso principal de scraping.", exc_info=True)

    logger.info("\n\n--- PROCESO DE SCRAPING COMPLETADO ---")


if __name__ == "__main__":
    try:
        # sys.stdout.reconfigure ya no es necesario con logging bien configurado
        main()
    except Exception as e:
        # Captura final por si algo falla fuera de main()
        # logger puede no estar inicializado aquí si falla muy temprano
        print(f"[FATAL] Unhandled exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)