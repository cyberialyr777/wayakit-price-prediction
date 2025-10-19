import xmlrpc.client
import os
from aiohttp import ClientError
import pandas as pd
import boto3
import json
import base64

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
        print(f"Error al obtener el secreto: {e}")
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
        print("ERROR: Faltan secretos esenciales de Odoo recuperados de AWS Secrets Manager.")
        exit()

    print(f"Secretos cargados exitosamente desde AWS Secrets Manager para DB: {ODOO_DB}")

except Exception as e:
    print(f"ERROR CRÍTICO: No se pudieron cargar los secretos. {e}")
    exit()

try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    print(f"Authentication successful. User ID (uid): {uid}")
except Exception as e:
    print(f"Authentication ERROR: {e}")
    exit()

MODEL_NAME = 'product.master'

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

print(f"\nSearching records in '{MODEL_NAME}' with advanced filters...")

try:
    records = models.execute_kw(
        ODOO_DB, uid, API_TOKEN,
        MODEL_NAME,
        'search_read',
        [domain],
        {'fields': fields_to_get}
    )
    print(f"Success. {len(records)} products found.")

except Exception as e:
    print(f"Critical ERROR during query: {e}")
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
    print(f"Unique combinations found: {len(df_unique)}")
    
    df_final = df_unique.copy()
    try:
        df_modifiers = pd.read_csv(MODIFIERS_FILE)

        if 'Type of product' in df_modifiers.columns and 'Search Modifiers' in df_modifiers.columns:
            df_final = pd.merge(df_unique, df_modifiers[['Type of product', 'Search Modifiers']], on='Type of product', how='left')
            df_final['Search Modifiers'] = df_final['Search Modifiers'].fillna('')
            print("Combination with Search Modifiers successful.")
        else:
            print(f"WARNING: The file '{MODIFIERS_FILE}' must have the columns 'Type of product' and 'Search Modifiers'.")
            df_final['Search Modifiers'] = ''

    except FileNotFoundError:
        print(f"WARNING: '{MODIFIERS_FILE}' not found. Report will be generated without Search Modifiers.")
        df_final['Search Modifiers'] = '' 
    except Exception as e:
        print(f"ERROR processing mapping file: {e}")
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
    print(f"\nSuccess! The file '{OUTPUT_CSV_FILE}' has been created with the final result.")

    print("\nPreview of the first 5 rows of the final result:")
    print(df_to_export.head())
    
else:
    print("No records found matching the filtering criteria.")

print("\nProcess completed.")