import xmlrpc.client
import os
from aiohttp import ClientError
import pandas as pd
import boto3
import json
import base64
from log_config import get_logger

logger = get_logger()

def get_secret(secret_name, region_name="me-south-1"):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        logger.error(f"Error al obtener el secreto", exc_info=True)
        raise e 
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(decoded_binary_secret)

SECRET_NAME = "wayakit/test/credentials" 
AWS_REGION = "me-south-1" 

try:
    secrets = get_secret(SECRET_NAME, AWS_REGION)

    ODOO_URL = secrets.get('ODOO_URL')
    ODOO_DB = secrets.get('ODOO_DB')
    ODOO_USERNAME = secrets.get('ODOO_USERNAME')
    API_TOKEN = secrets.get('ODOO_API_TOKEN')

    if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, API_TOKEN]):
        logger.error("ERROR: Faltan secretos esenciales de Odoo recuperados de AWS Secrets Manager.")
        exit()

    logger.info(f"Secretos cargados exitosamente desde AWS Secrets Manager para DB: {ODOO_DB}")

except Exception as e:
    logger.error(f"ERROR CRÍTICO: No se pudieron cargar los secretos.", exc_info=True)
    exit()

try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    logger.info(f"Authentication successful. User ID (uid): {uid}")
except Exception as e:
    logger.error(f"Authentication ERROR", exc_info=True)
    exit()

MODEL_NAME = 'product.master'

# Definir archivos de entrada y salida
MODIFIERS_FILE = 'modifiers_mapping.csv'
OUTPUT_CSV_FILE = 'analysis-odoo.csv'

products_to_exclude = [
    'F12-Other consumables for FM',
    'P11-Other consumables for pets',
    'H13-Other products, consumables or kits for homes'
]

domain = [
    ['status', '=', 'active'],
    ['subindustry_id.name', 'not in', ['Maritime', 'Defense']],
    ['type_of_product', 'not in', products_to_exclude]
]

fields_to_get = [
    'type_of_product',
    'subindustry_id',
    'industry_id',
    'generic_product_type',
]

logger.info(f"\nSearching records in '{MODEL_NAME}' with advanced filters...")

try:
    records = models.execute_kw(
        ODOO_DB, uid, API_TOKEN,
        MODEL_NAME,
        'search_read',
        [domain],
        {'fields': fields_to_get}
    )
    logger.info(f"Success. {len(records)} products found.")

except Exception as e:
    logger.error(f"Critical ERROR during query", exc_info=True)
    exit()

if records:
    
    df = pd.DataFrame(records)
    df['subindustry_id'] = df['subindustry_id'].apply(lambda x: x[1] if isinstance(x, list) else None)
    df['industry_id'] = df['industry_id'].apply(lambda x: x[1] if isinstance(x, list) else None)
    
    column_mapping = {
        'type_of_product': 'Type of product',
        'subindustry_id': 'Sub industry',
        'industry_id': 'Industry',
        'generic_product_type': 'Generic product type'
    }
    df_renamed = df.rename(columns=column_mapping)
    
    columns_for_uniqueness = [
        'Type of product', 'Sub industry', 'Industry', 'Generic product type'
    ]
    
    df_unique = df_renamed.drop_duplicates(subset=columns_for_uniqueness)
    logger.info(f"Unique combinations found: {len(df_unique)}")
    
    df_final = df_unique.copy()
    try:
        df_modifiers = pd.read_csv(MODIFIERS_FILE)

        if 'Type of product' in df_modifiers.columns and 'Search Modifiers' in df_modifiers.columns:
            df_final = pd.merge(df_unique, df_modifiers[['Type of product', 'Search Modifiers']], on='Type of product', how='left')
            df_final['Search Modifiers'] = df_final['Search Modifiers'].fillna('')
            logger.info("Combination with Search Modifiers successful.")
        else:
            logger.warning(f"WARNING: The file '{MODIFIERS_FILE}' must have the columns 'Type of product' and 'Search Modifiers'.")
            df_final['Search Modifiers'] = ''

    except FileNotFoundError:
        logger.warning(f"WARNING: '{MODIFIERS_FILE}' not found. Report will be generated without Search Modifiers.")
        df_final['Search Modifiers'] = '' 
    except Exception as e:
        logger.error(f"ERROR processing mapping file", exc_info=True)
        df_final['Search Modifiers'] = ''

    final_columns = [
        'Type of product',
        'Sub industry',
        'Industry',
        'Generic product type',
        'Search Modifiers'
    ]
    df_to_export = df_final[final_columns]
    
    df_to_export.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
    logger.info(f"\nSuccess! The file '{OUTPUT_CSV_FILE}' has been created with the final result.")

    logger.info("\nPreview of the first 5 rows of the final result:")
    logger.info(f"\n{df_to_export.head()}")
    
else:
    logger.warning("No records found matching the filtering criteria.")

logger.info("\nProcess completed.")