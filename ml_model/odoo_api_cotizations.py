import xmlrpc.client
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np # <-- 1. IMPORTAMOS NUMPY

# --- Carga de configuración (sin cambios) ---
load_dotenv()
ODOO_URL = os.getenv('ODOO_URL')
ODOO_USERNAME = os.getenv('ODOO_USERNAME')
API_TOKEN = os.getenv('ODOO_API_TOKEN')
ODOO_DB = os.getenv('ODOO_DB')

OUTPUT_CSV_FILE = 'wayakit_cotizations.csv'

if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, API_TOKEN]):
    print("ERROR: Faltan variables de entorno. Asegúrate de que tu archivo .env esté completo.")
    exit()

print(f"Configuración cargada para la base de datos: {ODOO_DB}")

# --- Autenticación en Odoo (sin cambios) ---
try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    print(f"Autenticación exitosa. User ID (uid): {uid}")
except Exception as e:
    print(f"ERROR de autenticación: {e}")
    exit()

# --- Modelo y Filtros (sin cambios) ---
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

print(f"\nBuscando registros en '{MODEL_NAME}' con filtro de descripción...")

try:
    records = models.execute_kw(
        ODOO_DB, uid, API_TOKEN,
        MODEL_NAME,
        'search_read',
        [domain],
        {'fields': fields_to_get}
    )
    print(f"Éxito. Se encontraron {len(records)} líneas de pedido que coinciden con el formato.")

except Exception as e:
    print(f"ERROR crítico durante la consulta: {e}")
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
    print(f"\n✅ ¡Éxito! El archivo '{OUTPUT_CSV_FILE}' ha sido creado.")

    print("\n📊 Vista previa de las primeras 5 filas del resultado final:")
    print(df_to_export.head())
    
else:
    print("No se encontraron registros que coincidan con todos los criterios de filtrado.")

print("\nProceso completado.")