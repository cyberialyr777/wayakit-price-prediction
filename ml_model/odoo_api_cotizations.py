import xmlrpc.client
import pandas as pd
import numpy as np
import boto3
import json
import os
import base64
from botocore.exceptions import ClientError
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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

OUTPUT_CSV_FILE = 'wayakit_cotizations.csv'

try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    logger.info(f"Autenticación exitosa. User ID (uid): {uid}")
except Exception as e:
    logger.error(f"ERROR de autenticación", exc_info=True)
    exit()

MODEL_NAME = 'sale.order.line'

domain = [
    ['order_id.state', '=', 'sale'],
    ['product_uom_qty', '>', 0],
    ['price_subtotal', '>', 0],
    ['name', 'like', '[FP-%']
]

fields_to_get = [
    'name',
    'product_uom_qty',
    'price_subtotal',
]

logger.info(f"\nBuscando registros en '{MODEL_NAME}' con filtro de descripción...")

try:
    records = models.execute_kw(
        ODOO_DB, uid, API_TOKEN,
        MODEL_NAME,
        'search_read',
        [domain],
        {'fields': fields_to_get}
    )
    logger.info(f"Éxito. Se encontraron {len(records)} líneas de pedido que coinciden con el formato.")

except Exception as e:
    logger.error(f"ERROR crítico durante la consulta", exc_info=True)
    exit()

# --- Procesamiento de datos (SECCIÓN MODIFICADA) ---
if records:
    df = pd.DataFrame(records)
    
    column_mapping = {
        'name': 'Description',
        'product_uom_qty': 'Quantity',
        'price_subtotal': 'Subtotal'
    }
    df_processed = df.rename(columns=column_mapping)
    
    df_processed['Product_ID'] = df_processed['Description'].str.extract(r'\[(.*?)\]')
    
    df_processed['Description'] = df_processed['Description'].str.replace(r'\[.*?\]\s*', '', regex=True)
    
    df_processed['approved_quote_price'] = df_processed['Subtotal'] / df_processed['Quantity']
    
    # --- 3. NUEVO: Actualizamos el orden final de las columnas ---
    final_columns = [
        'Product_ID',
        'Description',
        'approved_quote_price',
    ]
    df_to_export = df_processed[final_columns]
    
    # Guardar el resultado en un nuevo archivo CSV
    df_to_export.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
    logger.info(f"\n✅ ¡Éxito! El archivo '{OUTPUT_CSV_FILE}' ha sido creado.")

    logger.info("\n📊 Vista previa de las primeras 5 filas del resultado final:")
    logger.info(f"\n{df_to_export.head()}")
    
else:
    logger.warning("No se encontraron registros que coincidan con todos los criterios de filtrado.")

logger.info("\nProceso completado.")