    
import pandas as pd
import os
import sys
import argparse
# Añadir el directorio raíz al path para poder importar log_config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from log_config import get_logger

logger = get_logger()

# --- 2. AÑADIMOS EL PARSER DE ARGUMENTOS ---
parser = argparse.ArgumentParser(description="Prepara la lista de productos Wayakit para predecir.")
parser.add_argument('--run_mode', default='full', choices=['partial', 'full'],
                    help="Define el modo de ejecución: 'partial' (solo productos nuevos) o 'full' (todos los productos).")
args = parser.parse_args()
# --- FIN DE CAMBIOS 2 ---

def generar_lista_prediccion():
    archivo_volumetrico = 'ml_model/competitor_volumetric_processed.csv'
    archivo_unidades = 'ml_model/competitor_unit_processed.csv'
    # Decide qué catálogo de productos usar basado en el argumento --run_mode
    if args.run_mode == 'partial':
        catalogo_maestro = 'ml_model/wayakit_new_products_temp.csv'
        logger.info(f"Modo PARCIAL detectado. Usando catálogo de productos NUEVOS: '{catalogo_maestro}'")
    else:
        catalogo_maestro = 'ml_model/wayakit_products.csv'
        logger.info(f"Modo FULL detectado. Usando catálogo de productos COMPLETO: '{catalogo_maestro}'")
    # --- FIN DE CAMBIOS 3 ---
    archivo_salida = 'ml_model/wayakit_products_to_predict_odoo.csv'

    archivos_necesarios = [archivo_volumetrico, archivo_unidades, catalogo_maestro]
    for archivo in archivos_necesarios:
        if not os.path.exists(archivo):
            logger.error(f"File '{archivo}' not found.")
            logger.warning("Solution: Make sure you have executed the main notebook first.")
            return

    try:
        logger.info("STEP 1: Identifying products the model knows...")
        df_vol_comp = pd.read_csv(archivo_volumetrico)
        df_unit_comp = pd.read_csv(archivo_unidades)
        known_product_types = set(list(df_vol_comp['type_of_product'].unique()) + list(df_unit_comp['type_of_product'].unique()))
        logger.info(f"Identified {len(known_product_types)} product types that the model can predict.")

        logger.info("STEP 2: Loading master product catalog...")
        df_catalogue = pd.read_csv(catalogo_maestro, encoding='utf-8-sig')
        logger.info(f"Loaded {len(df_catalogue)} products from catalog.")

        logger.info("STEP 3: Filtering catalog to find compatible products...")
        df_catalogue['Type_of_product'] = df_catalogue['Type_of_product'].str.strip()
        df_predictable = df_catalogue[df_catalogue['Type_of_product'].isin(known_product_types)].copy()
        logger.info(f"Found {len(df_predictable)} compatible products.")
        
        logger.info(f"STEP 4: Saving new file as '{archivo_salida}'...")

        df_predictable.to_csv(archivo_salida, index=False)
        
        logger.info("Process completed!")
        logger.info(f"File '{archivo_salida}' is ready with filtered products and all original columns.")

    except FileNotFoundError:
        logger.error("One of the base files does not exist. Re-run the main notebook.", exc_info=True)
    except KeyError as e:
        logger.error(f"Column 'Type_of_product' not found: {e}", exc_info=True)
        logger.warning("Solution: Check that this column exists in your CSV files.")
    except Exception as e:
        logger.error("Unexpected error occurred during process", exc_info=True)

if __name__ == "__main__":
    generar_lista_prediccion()