# ml_model/odoo_api_competitor_products.py
import xmlrpc.client
import os
from botocore.exceptions import ClientError
import pandas as pd
import boto3
import json
import base64
import sys
# Add project root to path to import log_config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from log_config import get_logger

logger = get_logger() # Use specific logger name for this module

# --- AWS Secrets Manager ---
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
    except ClientError as e: # Catch boto3 ClientError specifically if aiohttp is not used elsewhere
        logger.error(f"Error getting secret '{secret_name}'", exc_info=True)
        raise e
    except Exception as e: # Generic exception for other potential errors
         logger.error(f"Unexpected error getting secret '{secret_name}'", exc_info=True)
         raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        elif 'SecretBinary' in get_secret_value_response:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(decoded_binary_secret)
        else:
            logger.error(f"Secret '{secret_name}' retrieved but contains no SecretString or SecretBinary.")
            return None

SECRET_NAME = "wayakit/test/credentials"
AWS_REGION = "me-south-1"

try:
    secrets = get_secret(SECRET_NAME, AWS_REGION)
    if not secrets:
        raise ValueError("Secrets could not be retrieved or were empty.")

    ODOO_URL = secrets.get('ODOO_URL')
    ODOO_DB = secrets.get('ODOO_DB')
    ODOO_USERNAME = secrets.get('ODOO_USERNAME')
    API_TOKEN = secrets.get('ODOO_API_TOKEN')

    if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, API_TOKEN]):
        missing = [k for k, v in {'URL': ODOO_URL, 'DB': ODOO_DB, 'Username': ODOO_USERNAME, 'Token': API_TOKEN}.items() if not v]
        logger.error(f"ERROR: Missing essential Odoo secrets from AWS Secrets Manager: {', '.join(missing)}.")
        exit(1) # Exit with error code

    logger.info(f"Secrets loaded successfully from AWS Secrets Manager for DB: {ODOO_DB}")

except Exception as e:
    logger.error(f"CRITICAL ERROR: Failed to load secrets. {e}", exc_info=True)
    exit(1) # Exit with error code

# --- Odoo Connection ---
try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    # Verify connection before authenticating
    version = common.version()
    logger.info(f"Connected to Odoo version: {version['server_version']}")

    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    if not uid:
         raise ValueError("Authentication failed, received empty UID.")
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    logger.info(f"Authentication successful. User ID (uid): {uid}")
except xmlrpc.client.ProtocolError as err:
    logger.error(f"Authentication ERROR: A protocol error occurred: {err.url}, {err.errcode}, {err.errmsg}", exc_info=False)
    exit(1)
except ConnectionRefusedError as e:
    logger.error(f"Authentication ERROR: Connection refused. Check Odoo URL ('{ODOO_URL}') and network.", exc_info=False)
    exit(1)
except Exception as e:
    logger.error(f"Authentication ERROR: An unexpected error occurred.", exc_info=True)
    exit(1)

# --- Fetch Data from Odoo ---
MODEL_NAME = 'competitor.product'
OUTPUT_CSV_FILE = 'ml_model/competitor_products_from_odoo.csv' # New output file name

# Define the fields to fetch based on the 'competitor.product' model and required output columns
fields_to_get = [
    'date',
    'product_channel', # Mapped to 'Channel'
    'classification_id', # This is Many2one, will fetch name later
    'product_category', # Related field, directly fetchable
    'subindustry_id', # This is Many2one, will fetch name later
    'industry_id', # This is Many2one, will fetch name later
    'generic_product_type', # Related field, directly fetchable
    'total_quantity', # Computed field, directly fetchable
    'uom', # Mapped to 'Unit of measurement [mL,g,units]'
    'price_unit_sar', # Computed monetary field, directly fetchable
    'company',
]

logger.info(f"\nSearching records in Odoo model '{MODEL_NAME}'...")

try:
    # Fetch all records without a specific domain for now, add filters if needed
    records = models.execute_kw(
        ODOO_DB, uid, API_TOKEN,
        MODEL_NAME,
        'search_read',
        [[]], # Empty domain fetches all records
        {'fields': fields_to_get}
    )
    logger.info(f"Success. {len(records)} competitor products found.")

except Exception as e:
    logger.error(f"Critical ERROR during Odoo query for '{MODEL_NAME}'", exc_info=True)
    exit(1)

# --- Process and Save Data ---
if records:
    df = pd.DataFrame(records)

    # Extract names from Many2one fields (returned as [id, 'name'])
    # Handle cases where the field might be False (empty)
    df['classification_id'] = df['classification_id'].apply(lambda x: x[1] if isinstance(x, list) and len(x) == 2 else None)
    df['subindustry_id'] = df['subindustry_id'].apply(lambda x: x[1] if isinstance(x, list) and len(x) == 2 else None)
    df['industry_id'] = df['industry_id'].apply(lambda x: x[1] if isinstance(x, list) and len(x) == 2 else None)

    # Map product_channel values
    channel_map = {
        'b2b': 'B2B',
        'retail': 'B2C', # Or map as needed, e.g., B2C
        'ecommerce': 'B2C' # Example mapping, adjust as per your logic
    }
    df['product_channel'] = df['product_channel'].map(channel_map).fillna('Unknown') # Handle missing/unexpected values

    # Map uom values
    uom_map = {
        'ml': 'mL',
        'g': 'g',
        'units': 'Units'
    }
    df['uom'] = df['uom'].map(uom_map).fillna('Unknown')

    # Rename columns to match the desired CSV header
    column_mapping = {
        'date': 'Date',
        'product_channel': 'Channel',
        'classification_id': 'Type of product', # Using classification name as Type of product
        'product_category': 'Product category',
        'subindustry_id': 'Sub industry',
        'industry_id': 'Industry',
        'generic_product_type': 'Generic product type',
        'total_quantity': 'Total quantity',
        'uom': 'Unit of measurement [mL,g,units]',
        'price_unit_sar': 'Price per unit SAR',
        'company': 'Company',
    }
    df_renamed = df.rename(columns=column_mapping)

    # Ensure all required columns exist, adding any missing ones with default values (e.g., None or '')
    final_columns_order = [
        'Date','Channel','Type of product','Product category','Sub industry',
        'Industry','Generic product type','Total quantity',
        'Unit of measurement [mL,g,units]','Price per unit SAR','Company'
    ]
    for col in final_columns_order:
        if col not in df_renamed.columns:
            logger.warning(f"Column '{col}' not found in fetched data. Adding it as empty.")
            df_renamed[col] = None # Or use pd.NA or '' depending on expected type

    # Select and reorder columns
    df_final = df_renamed[final_columns_order]

    # Format Date column if needed (Odoo typically returns YYYY-MM-DD)
    df_final['Date'] = pd.to_datetime(df_final['Date']).dt.strftime('%d/%m/%Y') # Format as DD/MM/YYYY

    # Format Price column (optional, keep as number is usually better for processing)
    # df_final['Price per unit SAR'] = df_final['Price per unit SAR'].apply(lambda x: f"SAR {x:,.2f}" if pd.notna(x) else '')

    # Save to CSV
    try:
        df_final.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
        logger.info(f"\nSuccess! The file '{OUTPUT_CSV_FILE}' has been created with data from Odoo.")
        logger.info("\nPreview of the first 5 rows:")
        logger.info(f"\n{df_final.head()}")
    except Exception as e:
        logger.error(f"Error saving data to CSV '{OUTPUT_CSV_FILE}'", exc_info=True)
        exit(1)

else:
    logger.warning("No competitor product records found in Odoo.")

logger.info("\nOdoo competitor product fetching process completed.")