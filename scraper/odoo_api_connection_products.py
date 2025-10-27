import xmlrpc.client
import os
import pandas as pd
import argparse
import boto3
import json
import base64
from botocore.exceptions import ClientError
from log_config import get_logger

logger = get_logger()

def get_secret(secret_name, region_name="me-south-1"):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Error al obtener el secreto '{secret_name}': {e}", exc_info=True)
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(decoded_binary_secret)


parser = argparse.ArgumentParser(description="Genera la lista de productos/subindustrias para scraping.")
parser.add_argument('--input_odoo_products_file', default='../ml/wayakit_products.csv',
                    help="Archivo CSV de entrada con productos de Odoo (ej. wayakit_products.csv o wayakit_new_products_temp.csv).")
parser.add_argument('--output_analysis_file', default='analysis-odoo.csv',
                    help="Archivo CSV de salida para el scraper (ej. analysis-odoo.csv o analysis-odoo_partial.csv).")
parser.add_argument('--modifiers_file', default='modifiers_mapping.csv',
                    help="Archivo CSV con mapeo de modificadores de búsqueda.")
args = parser.parse_args()

SECRET_NAME = "wayakit/test/credentials"
AWS_REGION = "me-south-1"
try:
    secrets = get_secret(SECRET_NAME, AWS_REGION)
    ODOO_URL = secrets.get('ODOO_URL')
    ODOO_DB = secrets.get('ODOO_DB')
    ODOO_USERNAME = secrets.get('ODOO_USERNAME')
    API_TOKEN = secrets.get('ODOO_API_TOKEN')
    if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, API_TOKEN]):
        logger.error("Faltan secretos esenciales de Odoo recuperados.")
        exit(1)
    logger.info(f"Secretos cargados para DB: {ODOO_DB}")
except Exception as e:
    logger.critical(f"No se pudieron cargar los secretos.", exc_info=True)
    exit(1)

INPUT_ODOO_FILE = args.input_odoo_products_file
OUTPUT_ANALYSIS_FILE = args.output_analysis_file
MODIFIERS_FILE = args.modifiers_file

try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    logger.info(f"Autenticación Odoo exitosa. UID: {uid}")
except Exception as e:
    logger.error(f"Error de autenticación Odoo.", exc_info=True)
    exit(1)

logger.info(f"Leyendo archivo de productos Odoo: '{INPUT_ODOO_FILE}'")
try:
    df_odoo_products = pd.read_csv(INPUT_ODOO_FILE)
    logger.info(f"Leídos {len(df_odoo_products)} productos desde '{INPUT_ODOO_FILE}'.")

    column_mapping = {
        'Type_of_product': 'Type of product',
        'SubIndustry': 'Sub industry',
        'Industry': 'Industry',
        'Generic product type': 'Generic product type'
    }
    df_renamed = df_odoo_products.rename(columns=column_mapping)

    columns_for_uniqueness = [
        'Type of product', 'Sub industry', 'Industry', 'Generic product type'
    ]
    missing_cols = [col for col in columns_for_uniqueness if col not in df_renamed.columns]
    if missing_cols:
        logger.error(f"Faltan columnas requeridas en '{INPUT_ODOO_FILE}': {missing_cols}")
        exit(1)

    df_unique = df_renamed.drop_duplicates(subset=columns_for_uniqueness)
    logger.info(f"Combinaciones únicas encontradas: {len(df_unique)}")

    df_final = df_unique.copy()
    try:
        df_modifiers = pd.read_csv(MODIFIERS_FILE)
        if 'Type of product' in df_modifiers.columns and 'Search Modifiers' in df_modifiers.columns:
            df_final['Type of product'] = df_final['Type of product'].astype(str)
            df_modifiers['Type of product'] = df_modifiers['Type of product'].astype(str)

            df_final = pd.merge(df_final, df_modifiers[['Type of product', 'Search Modifiers']], on='Type of product', how='left')
            df_final['Search Modifiers'] = df_final['Search Modifiers'].fillna('')
            logger.info("Combinación con Search Modifiers exitosa.")
        else:
            logger.warning(f"El archivo '{MODIFIERS_FILE}' debe tener 'Type of product' y 'Search Modifiers'. No se añadirán modificadores.")
            df_final['Search Modifiers'] = ''
    except FileNotFoundError:
        logger.warning(f"'{MODIFIERS_FILE}' no encontrado. No se añadirán modificadores.")
        df_final['Search Modifiers'] = ''
    except Exception as e:
        logger.error(f"Error procesando archivo de modificadores '{MODIFIERS_FILE}'.", exc_info=True)
        df_final['Search Modifiers'] = ''

    final_columns = [
        'Type of product', 'Sub industry', 'Industry', 'Generic product type', 'Search Modifiers'
    ]
    final_columns_exist = [col for col in final_columns if col in df_final.columns]
    df_to_export = df_final[final_columns_exist] 

    df_to_export.to_csv(OUTPUT_ANALYSIS_FILE, index=False, encoding='utf-8-sig')
    logger.info(f"¡Éxito! Archivo de análisis generado: '{OUTPUT_ANALYSIS_FILE}'")
    logger.info("Vista previa:")
    logger.info("\n" + df_to_export.head().to_string()) 

except FileNotFoundError:
    logger.error(f"Archivo de entrada '{INPUT_ODOO_FILE}' no encontrado.")
    exit(1)
except Exception as e:
    logger.critical(f"Error inesperado procesando productos.", exc_info=True)
    exit(1)

logger.info("Proceso completado.")