import xmlrpc.client
import os
from dotenv import load_dotenv
import pandas as pd

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# --- 1. CONFIGURACIÓN DE CONEXIÓN A ODOO ---
ODOO_URL = os.getenv('ODOO_URL')
ODOO_DB = os.getenv('ODOO_DB')
ODOO_USERNAME = os.getenv('ODOO_USERNAME')
API_TOKEN = os.getenv('ODOO_API_TOKEN')

# Nombre del archivo de salida
OUTPUT_CSV_FILE = 'wayakit_products.csv'

# Validar que las credenciales existan
if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, API_TOKEN]):
    print("❌ ERROR: Faltan variables de entorno. Asegúrate de que ODOO_URL, ODOO_DB, ODOO_USERNAME y API_TOKEN estén en tu archivo .env.")
    exit()

print(f"✅ Configuración cargada para la base de datos: {ODOO_DB}")

# --- 2. AUTENTICACIÓN CON ODOO ---
try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    print(f"🔑 Autenticación exitosa. User ID (uid): {uid}")
except Exception as e:
    print(f"🔥 ERROR de autenticación: {e}")
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

print(f"\n🔎 Buscando registros en el modelo '{MODEL_NAME}'...")

# --- 4. EJECUCIÓN DE LA CONSULTA ---
try:
    records = models.execute_kw(
        ODOO_DB, uid, API_TOKEN,
        MODEL_NAME,
        'search_read',
        [domain],
        {'fields': fields_to_get}
    )
    print(f"👍 Éxito. Se encontraron {len(records)} registros.")

except Exception as e:
    print(f"🔥 ERROR crítico durante la consulta: {e}")
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
    print(f"\n✅ ¡Éxito! El archivo '{OUTPUT_CSV_FILE}' ha sido creado con el resultado final.")

    print("\n📊 Vista previa de las primeras 5 filas del resultado:")
    print(df_final.head())
    
else:
    print("🤷 No se encontraron registros que coincidan con los criterios de filtrado.")

print("\nProceso completado.")