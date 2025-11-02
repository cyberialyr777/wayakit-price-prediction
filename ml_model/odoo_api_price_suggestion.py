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

# import xmlrpc.client
# from botocore.exceptions import ClientError
# import pandas as pd
# import os
# from datetime import datetime
# import boto3
# import json
# import base64
# import sys
# import argparse # <-- 1. Importar argparse
# from datetime import datetime

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from log_config import get_logger

# logger = get_logger()

# # --- Nombres de los Modelos de Odoo ---
# MODEL_NAME = 'product.price.suggestion'
# HISTORY_MODEL_NAME = 'product.price.suggestion.history' # <-- El nuevo modelo que crearás

# def get_secret(secret_name, region_name="me-south-1"):
#     session = boto3.session.Session()
#     client = session.client(
#         service_name='secretsmanager',
#         region_name=region_name
#     )
#     try:
#         get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name
#         )
#     except ClientError as e:
#         logger.error(f"Error al obtener el secreto", exc_info=True)
#         raise e 
#     else:
#         if 'SecretString' in get_secret_value_response:
#             secret = get_secret_value_response['SecretString']
#             return json.loads(secret)
#         else:
#             decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
#             return json.loads(decoded_binary_secret)

# # --- Carga de Secretos ---
# SECRET_NAME = "wayakit/test/credentials" 
# AWS_REGION = "me-south-1" 
# try:
#     secrets = get_secret(SECRET_NAME, AWS_REGION)
#     ODOO_URL = secrets.get('ODOO_URL')
#     ODOO_DB = secrets.get('ODOO_DB')
#     ODOO_USERNAME = secrets.get('ODOO_USERNAME')
#     API_TOKEN = secrets.get('ODOO_API_TOKEN')
#     if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, API_TOKEN]):
#         logger.error("ERROR: Faltan secretos esenciales de Odoo.")
#         exit()
#     logger.info(f"Secretos cargados exitosamente para DB: {ODOO_DB}")
# except Exception as e:
#     logger.error(f"ERROR CRÍTICO: No se pudieron cargar los secretos.", exc_info=True)
#     exit()

# # --- Conexión Odoo ---
# try:
#     common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
#     uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
#     models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
#     logger.info(f"Autenticación exitosa. User ID (uid): {uid}")
# except Exception as e:
#     logger.error(f"ERROR de autenticación", exc_info=True)
#     exit()

# # --- 1. Definir Argumentos ---
# parser = argparse.ArgumentParser(description="Carga sugerencias de precios a Odoo.")
# parser.add_argument('--run_mode', default='partial', choices=['partial', 'full'],
#                     help="Define el modo de ejecución: 'partial' (solo añade) o 'full' (archiva, borra y añade).")
# args = parser.parse_args()


# # --- 2. Cargar Nuevas Predicciones (Paso de seguridad) ---
# # Leemos el archivo CSV ANTES de borrar nada.
# csv_file_path = 'ml_model/wayakit_prediction_report.csv'
# try:
#     df_results = pd.read_csv(csv_file_path)
#     logger.info(f"Archivo CSV '{csv_file_path}' cargado. Se encontraron {len(df_results)} filas.")
# except FileNotFoundError:
#     logger.error(f"ERROR: El archivo '{csv_file_path}' no se encontró. No se subirá nada a Odoo.")
#     exit()

# # Preparar los datos del CSV
# all_records_data = []
# for index, row in df_results.iterrows():
#     try:
#         volume_str = str(row['volume']).split(' ')[0]
#         volume_float = float(volume_str)

#         record_data = {
#             'product_id_str': row['Product_ID'],
#             'product_type': row['product_type'],
#             'generic_product_type': row['generic_product_type'],
#             'subindustry': row['subindustry'],
#             'industry': row['industry'],
#             'volume_units': volume_float,
#             'production_cost': float(row['cost_per_unit']),
#             'suggested_price': float(row['predicted_price']),
#             'profit': float(row['porcentaje_de_ganancia']),
#         }
#         all_records_data.append(record_data)
#     except Exception as e:
#         logger.warning(f"Error al procesar la fila {index+1} (Product ID: {row.get('Product_ID', 'N/A')}). Saltando registro.", exc_info=True)

# # Si el CSV estaba vacío o falló al procesar filas, no continuar.
# if not all_records_data:
#     logger.warning("No hay registros válidos para cargar desde el CSV. Proceso finalizado.")
#     exit()

# # --- 3. Lógica de Archivado y Borrado (Solo para modo 'full') ---
# if args.run_mode == 'full':
#     logger.info("--- MODO EJECUCIÓN: FULL ---")
#     logger.info(f"Iniciando archivado de '{MODEL_NAME}' a '{HISTORY_MODEL_NAME}'...")
#     try:
#         # 1. Buscar todos los IDs de los registros existentes
#         existing_ids = models.execute_kw(
#             ODOO_DB, uid, API_TOKEN,
#             MODEL_NAME, 'search', [[('id', '!=', 0)]] 
#         )
        
#         if existing_ids:
#             logger.info(f"Se encontraron {len(existing_ids)} registros para archivar.")
            
#             # 2. Leer los datos de esos registros
#             fields_to_archive = [
#                 'product_id_str', 'product_type', 'generic_product_type', 
#                 'subindustry', 'industry', 'volume_units', 
#                 'production_cost', 'suggested_price', 'profit', 'create_date'
#             ]
#             old_records = models.execute_kw(
#                 ODOO_DB, uid, API_TOKEN,
#                 MODEL_NAME, 'read', [existing_ids], {'fields': fields_to_archive}
#             )
            
#             # 3. Preparar datos para el historial (quitar 'id')
#             records_to_create_in_history = []
#             for record in old_records:
#                 record.pop('id', None) # Quitar el ID para que Odoo cree uno nuevo
#                 records_to_create_in_history.append(record)
            
#             # 4. Crear los registros en el modelo de historial
#             logger.info(f"Creando {len(records_to_create_in_history)} registros en el historial '{HISTORY_MODEL_NAME}'...")
#             models.execute_kw(
#                 ODOO_DB, uid, API_TOKEN,
#                 HISTORY_MODEL_NAME, 'create', [records_to_create_in_history]
#             )
#             logger.info("¡Registros archivados en historial exitosamente!")

#             # 5. Borrar los registros del modelo principal
#             logger.info(f"Borrando {len(existing_ids)} registros antiguos de '{MODEL_NAME}'...")
#             models.execute_kw(
#                 ODOO_DB, uid, API_TOKEN,
#                 MODEL_NAME, 'unlink', [existing_ids]
#             )
#             logger.info("¡Registros antiguos borrados exitosamente!")

#         else:
#             logger.info("No se encontraron registros existentes. No se requiere archivado.")

#     except Exception as e:
#         logger.error(f"ERROR CRÍTICO durante el archivado/borrado.", exc_info=True)
#         logger.error("Abortando para prevenir carga duplicada. La tabla de sugerencias puede no estar limpia.")
#         exit(1) # Salir con error

# else:
#     logger.info("--- MODO EJECUCIÓN: PARTIAL ---")
#     logger.info("Solo se añadirán los nuevos registros (sin borrar).")


# # --- 4. Cargar Nuevos Registros (Se ejecuta en AMBOS modos) ---
# logger.info(f"\n🚀 Enviando {len(all_records_data)} registros nuevos a '{MODEL_NAME}' en una sola llamada...")
# try:
#     new_record_ids = models.execute_kw(
#         ODOO_DB, uid, API_TOKEN,
#         MODEL_NAME,
#         'create',
#         [all_records_data]
#     )
    
#     logger.info(f"¡Carga masiva completada con éxito!")
#     logger.info(f"   Se crearon {len(new_record_ids)} nuevos registros.")

# except Exception as e:
#     logger.error(f"ERROR CRÍTICO durante la carga masiva", exc_info=True)
#     logger.error("   La operación falló. Ningún registro fue creado en este lote.")
    
# logger.info("\nProceso finalizado.")