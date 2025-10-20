import xmlrpc.client
import os
from aiohttp import ClientError
import pandas as pd
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

# Nombre del archivo de salida
OUTPUT_CSV_FILE = 'wayakit_products.csv'

# --- 2. AUTENTICACIÓN CON ODOO ---
try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    logger.info(f"🔑 Autenticación exitosa. User ID (uid): {uid}")
except Exception as e:
    logger.error(f"🔥 ERROR de autenticación", exc_info=True)
    exit()

# --- 3. DEFINICIÓN DE LA CONSULTA ---
MODEL_NAME = 'product.master'

# Filtros para la búsqueda (mantenidos de tu script original)
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

# Campos a obtener del modelo 'product.master'
fields_to_get = [
    'product_id',
    'product_name',
    'label_product_name',
    'presentation',
    'volume_liters',
    'pack_quantity_units',
    'type_of_product',
    'category',
    'generic_product_type',
    'subindustry_id',
    'industry_id',
    'bottle_cost',
    'label_cost',
    'liquid_cost',
    'microfibers_cost',
    'plastic_bag_cost',
    'labor_cost',
    'shipping_cost',
    'other_costs',
    'unit_cost_sar',
]

logger.info(f"\n🔎 Buscando registros en el modelo '{MODEL_NAME}'...")

# --- 4. EJECUCIÓN DE LA CONSULTA ---
try:
    records = models.execute_kw(
        ODOO_DB, uid, API_TOKEN,
        MODEL_NAME,
        'search_read',
        [domain],
        {'fields': fields_to_get}
    )
    logger.info(f"👍 Éxito. Se encontraron {len(records)} registros.")

except Exception as e:
    logger.error(f"🔥 ERROR crítico durante la consulta", exc_info=True)
    exit()

# --- 5. PROCESAMIENTO DE DATOS Y EXPORTACIÓN ---
if records:
    # Convertir los registros a un DataFrame de pandas
    df = pd.DataFrame(records)
    
    # Odoo devuelve los campos Many2one como una lista [id, 'nombre'].
    # Esta función extrae solo el nombre.
    df['subindustry_id'] = df['subindustry_id'].apply(lambda x: x[1] if isinstance(x, list) else None)
    df['industry_id'] = df['industry_id'].apply(lambda x: x[1] if isinstance(x, list) else None)

    # Mapeo de los nombres de campo de Odoo a los nombres de columna deseados en el CSV
    column_mapping = {
        'product_id': 'Product_ID',
        'product_name': 'Product_Name',
        'label_product_name': 'Label_Product_Name',
        'presentation': 'Presentation',
        'volume_liters': 'Volume_Liters',
        'pack_quantity_units': 'Pack_quantity_Units',
        'type_of_product': 'Type_of_product',
        'category': 'Category',
        'generic_product_type': 'Generic product type',
        'subindustry_id': 'SubIndustry',
        'industry_id': 'Industry',
        'bottle_cost': 'Bottle',
        'label_cost': 'Label',
        'liquid_cost': 'Liquid',
        'microfibers_cost': 'Microfibers',
        'plastic_bag_cost': 'Plastic bag',
        'labor_cost': 'Labor',
        'shipping_cost': 'Shipping',
        'other_costs': 'Other costs',
        'unit_cost_sar': 'Unit_cost_SAR'
    }
    
    df_renamed = df.rename(columns=column_mapping)

    # Definir el orden final de las columnas para el archivo CSV
    final_columns_order = [
        'Product_ID', 'Product_Name', 'Label_Product_Name', 'Presentation', 
        'Volume_Liters', 'Pack_quantity_Units', 'Type_of_product', 'Category', 
        'Generic product type', 'SubIndustry', 'Industry', 'Bottle', 'Label', 
        'Liquid', 'Microfibers', 'Plastic bag', 'Labor', 'Shipping', 
        'Other costs', 'Unit_cost_SAR'
    ]

    # Reordenar el DataFrame para que coincida con el orden solicitado
    df_final = df_renamed[final_columns_order]
    
    # Guardar el DataFrame final en un archivo CSV
    df_final.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
    logger.info(f"\n✅ ¡Éxito! El archivo '{OUTPUT_CSV_FILE}' ha sido creado con el resultado final.")

    logger.info("\n📊 Vista previa de las primeras 5 filas del resultado:")
    logger.info(f"\n{df_final.head()}")
    
else:
    logger.warning("🤷 No se encontraron registros que coincidan con los criterios de filtrado.")

logger.info("\nProceso completado.")