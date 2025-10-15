import xmlrpc.client
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
ODOO_URL = os.getenv('ODOO_URL')
ODOO_USERNAME = os.getenv('ODOO_USERNAME')
API_TOKEN = os.getenv('ODOO_API_TOKEN')


ODOO_DB = ODOO_URL.split('//')[1].split('.')[0]

print("--- Iniciando Script de Carga de Datos a Odoo ---")

if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, API_TOKEN]):
    print("❌ ERROR: Faltan variables en tu archivo .env.")
    print("   Asegúrate de tener ODOO_URL, ODOO_USERNAME y ODOO_API_TOKEN.")
    exit()

print(f"✅ Configuración cargada para DB: {ODOO_DB}")

try:
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, API_TOKEN, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    print(f"✅ Autenticación exitosa. User ID (uid): {uid}")

except Exception as e:
    print(f"❌ ERROR de autenticación: {e}")
    exit()

csv_file_path = 'C:\\Users\\barbe\\OneDrive\\Documentos\\wayakit\\ml-prediction-price\\notebooks\\wayakit_prediction_report_final_explicado.csv'
try:
    df_results = pd.read_csv(csv_file_path)
    print(f"📄 Archivo CSV '{csv_file_path}' cargado. Se encontraron {len(df_results)} filas.")
except FileNotFoundError:
    print(f"❌ ERROR: El archivo '{csv_file_path}' no se encontró.")
    exit()

MODEL_NAME = 'product.price.suggestion'
all_records_data = []

print(f"\n⚙️ Preparando {len(df_results)} registros para la carga masiva...")

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
        print(f"  ⚠️ Error al procesar la fila {index+1} (Product ID: {row.get('Product_ID', 'N/A')}). Saltando registro. Error: {e}")

if all_records_data:
    print(f"\n🚀 Enviando {len(all_records_data)} registros a Odoo en una sola llamada...")
    try:
        new_record_ids = models.execute_kw(
            ODOO_DB, uid, API_TOKEN,
            MODEL_NAME,
            'create',
            [all_records_data]
        )
        
        print(f"✅ ¡Carga masiva completada con éxito!")
        print(f"   Se crearon {len(new_record_ids)} nuevos registros.")

    except Exception as e:
        print(f"❌ ERROR CRÍTICO durante la carga masiva: {e}")
        print("   La operación falló. Ningún registro fue creado en este lote.")
else:
    print("🤷 No se prepararon registros para la carga. Revisa si hubo errores en el procesamiento de filas.")

print("\n🎉 Proceso finalizado.")