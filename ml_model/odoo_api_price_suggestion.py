import xmlrpc.client
from botocore.exceptions import ClientError
import pandas as pd
import os
from datetime import datetime
import boto3
import json
import base64
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

try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    logger.info(f"Autenticación exitosa. User ID (uid): {uid}")

except Exception as e:
    logger.error(f"ERROR de autenticación", exc_info=True)
    exit()

csv_file_path = 'ml_model/wayakit_prediction_report.csv'
try:
    df_results = pd.read_csv(csv_file_path)
    logger.info(f"Archivo CSV '{csv_file_path}' cargado. Se encontraron {len(df_results)} filas.")
except FileNotFoundError:
    logger.error(f"ERROR: El archivo '{csv_file_path}' no se encontró.")
    exit()

MODEL_NAME = 'product.price.suggestion'
all_records_data = []

logger.info(f"\nPreparando {len(df_results)} registros para la carga masiva...")

for index, row in df_results.iterrows():
    try:
        volume_str = str(row['volume']).split(' ')[0]
        volume_float = float(volume_str)

        record_data = {
            'product_id_str': row['Product_ID'],
            'product_type': row['product_type'],
            'generic_product_type': row['generic_product_type'],
            'subindustry': row['subindustry'],
            'industry': row['industry'],
            'volume_units': volume_float,
            'production_cost': float(row['cost_per_unit']),
            'suggested_price': float(row['predicted_price']),
            'profit': float(row['porcentaje_de_ganancia']),
        }
        all_records_data.append(record_data)

    except Exception as e:
        logger.warning(f"Error al procesar la fila {index+1} (Product ID: {row.get('Product_ID', 'N/A')}). Saltando registro.", exc_info=True)

if all_records_data:
    logger.info(f"\n🚀 Enviando {len(all_records_data)} registros a Odoo en una sola llamada...")
    try:
        new_record_ids = models.execute_kw(
            ODOO_DB, uid, API_TOKEN,
            MODEL_NAME,
            'create',
            [all_records_data]
        )
        
        logger.info(f"¡Carga masiva completada con éxito!")
        logger.info(f"   Se crearon {len(new_record_ids)} nuevos registros.")

    except Exception as e:
        logger.error(f"ERROR CRÍTICO durante la carga masiva", exc_info=True)
        logger.error("   La operación falló. Ningún registro fue creado en este lote.")
else:
    logger.warning("No se prepararon registros para la carga. Revisa si hubo errores en el procesamiento de filas.")

logger.info("\nProceso finalizado.")