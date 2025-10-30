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

OUTPUT_CSV_FILE = 'ml_model/wayakit_cotizations.csv'

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
    ['name', 'like', '[FP-%'],
    ['order_id.date_order', '>=', '2025-01-01 00:00:00'],
    ['order_id.date_order', '<=', '2025-12-31 23:59:59'],
]

fields_to_get = [
    'name',
    'product_uom_qty',
    'price_subtotal',
]

logger.info(f"\nSearching records in '{MODEL_NAME}' for year 2025...")

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

# import xmlrpc.client
# import pandas as pd
# import numpy as np
# import boto3
# import json
# import os
# import base64
# from botocore.exceptions import ClientError
# import sys
# # Add project root to path to import log_config
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from log_config import get_logger

# # Use a specific logger name for this module
# logger = get_logger(__name__)

# # --- AWS Secrets Manager ---
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
#     # Catch specific boto3 ClientError
#     except ClientError as e:
#         logger.error(f"Error getting secret '{secret_name}'", exc_info=True)
#         raise e
#     # Generic exception for other potential errors
#     except Exception as e:
#          logger.error(f"Unexpected error getting secret '{secret_name}'", exc_info=True)
#          raise e
#     else:
#         if 'SecretString' in get_secret_value_response:
#             secret = get_secret_value_response['SecretString']
#             return json.loads(secret)
#         elif 'SecretBinary' in get_secret_value_response:
#             decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
#             return json.loads(decoded_binary_secret)
#         else:
#             logger.error(f"Secret '{secret_name}' retrieved but contains no SecretString or SecretBinary.")
#             return None

# SECRET_NAME = "wayakit/test/credentials"
# AWS_REGION = "me-south-1"

# try:
#     secrets = get_secret(SECRET_NAME, AWS_REGION)
#     if not secrets:
#         raise ValueError("Secrets could not be retrieved or were empty.")

#     ODOO_URL = secrets.get('ODOO_URL')
#     ODOO_DB = secrets.get('ODOO_DB')
#     ODOO_USERNAME = secrets.get('ODOO_USERNAME')
#     API_TOKEN = secrets.get('ODOO_API_TOKEN')

#     if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, API_TOKEN]):
#         missing = [k for k, v in {'URL': ODOO_URL, 'DB': ODOO_DB, 'Username': ODOO_USERNAME, 'Token': API_TOKEN}.items() if not v]
#         logger.error(f"ERROR: Missing essential Odoo secrets from AWS Secrets Manager: {', '.join(missing)}.")
#         # Exit with error code
#         exit(1)

#     logger.info(f"Secrets loaded successfully from AWS Secrets Manager for DB: {ODOO_DB}")

# except Exception as e:
#     logger.error(f"CRITICAL ERROR: Failed to load secrets. {e}", exc_info=True)
#     # Exit with error code
#     exit(1)

# OUTPUT_CSV_FILE = 'wayakit_cotizations.csv'

# # --- Odoo Connection ---
# try:
#     common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
#     # Verify connection before authenticating
#     version = common.version()
#     logger.info(f"Connected to Odoo version: {version['server_version']}")

#     uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
#     if not uid:
#          raise ValueError("Authentication failed, received empty UID.")
#     models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
#     logger.info(f"Authentication successful. User ID (uid): {uid}")
# except xmlrpc.client.ProtocolError as err:
#     logger.error(f"Authentication ERROR: A protocol error occurred: {err.url}, {err.errcode}, {err.errmsg}", exc_info=False)
#     exit(1)
# except ConnectionRefusedError as e:
#     logger.error(f"Authentication ERROR: Connection refused. Check Odoo URL ('{ODOO_URL}') and network.", exc_info=False)
#     exit(1)
# except Exception as e:
#     logger.error(f"Authentication ERROR: An unexpected error occurred.", exc_info=True)
#     exit(1)

# # --- Fetch Data from Odoo ---
# MODEL_NAME = 'sale.order.line'

# # MODIFIED domain: Added date filter for 2025
# domain = [
#     ['order_id.state', '=', 'sale'],
#     ['product_uom_qty', '>', 0],
#     ['price_subtotal', '>', 0],
#     ['name', 'like', '[FP-%'],
#     # Filter by order date (date_order on sale.order model) for the year 2025
#     ['order_id.date_order', '>=', '2025-01-01 00:00:00'],
#     ['order_id.date_order', '<=', '2025-12-31 23:59:59'],
# ]

# # MODIFIED fields_to_get: Added 'order_id.date_order' to fetch the date
# fields_to_get = [
#     'name',
#     'product_uom_qty',
#     'price_subtotal',
#     'order_id', # Needed to access related field
#     'order_id.date_order', # Fetch the date from the related sale.order
# ]

# logger.info(f"\nSearching records in '{MODEL_NAME}' for year 2025...")

# try:
#     records = models.execute_kw(
#         ODOO_DB, uid, API_TOKEN,
#         MODEL_NAME,
#         'search_read',
#         # Pass the domain list directly
#         [domain],
#         {'fields': fields_to_get}
#     )
#     logger.info(f"Success. Found {len(records)} order lines matching the criteria for 2025.")

# except Exception as e:
#     logger.error(f"Critical ERROR during Odoo query for '{MODEL_NAME}'", exc_info=True)
#     exit(1)

# # --- Data Processing ---
# if records:
#     df = pd.DataFrame(records)

#     # Clean up relational field representation if necessary
#     # Odoo returns Many2one as [id, name], we only need the id for date_order link
#     df['order_id'] = df['order_id'].apply(lambda x: x[0] if isinstance(x, list) and len(x) >= 1 else None)

#     # Rename columns for clarity before processing
#     column_mapping = {
#         'name': 'Description',
#         'product_uom_qty': 'Quantity',
#         'price_subtotal': 'Subtotal',
#         # Keep date temporarily if needed for verification, but won't be in final CSV
#         # 'order_id.date_order': 'Order_Date'
#     }
#     df_processed = df.rename(columns=column_mapping)

#     # Extract Product_ID
#     df_processed['Product_ID'] = df_processed['Description'].str.extract(r'\[(.*?)\]')

#     # Clean Description
#     df_processed['Description'] = df_processed['Description'].str.replace(r'\[.*?\]\s*', '', regex=True)

#     # Calculate approved_quote_price
#     # Ensure Quantity is numeric and handle potential division by zero
#     df_processed['Quantity'] = pd.to_numeric(df_processed['Quantity'], errors='coerce')
#     df_processed['Subtotal'] = pd.to_numeric(df_processed['Subtotal'], errors='coerce')
#     df_processed['approved_quote_price'] = df_processed.apply(
#         lambda row: row['Subtotal'] / row['Quantity'] if pd.notna(row['Quantity']) and row['Quantity'] != 0 else 0,
#         axis=1
#     )

#     # --- Select final columns (excluding date) ---
#     final_columns = [
#         'Product_ID',
#         'Description',
#         'approved_quote_price',
#     ]
#     # Ensure Product_ID column exists before selecting
#     if 'Product_ID' not in df_processed.columns:
#         logger.error("Column 'Product_ID' could not be extracted. Check data format.")
#         exit(1)

#     df_to_export = df_processed[final_columns]

#     # Drop rows where Product_ID might be NaN after extraction
#     df_to_export.dropna(subset=['Product_ID'], inplace=True)


#     # --- Save to CSV ---
#     try:
#         df_to_export.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
#         logger.info(f"\n✅ Success! The file '{OUTPUT_CSV_FILE}' has been created with 2025 quotations.")
#         logger.info("\n📊 Preview of the first 5 rows of the final result:")
#         logger.info(f"\n{df_to_export.head()}")
#     except Exception as e:
#         logger.error(f"Error saving data to CSV '{OUTPUT_CSV_FILE}'", exc_info=True)
#         exit(1)

# else:
#     logger.warning("No quotation records found matching all filtering criteria for 2025.")

# logger.info("\nProcess completed.")