# ml_model/filter_new_products.py
import pandas as pd
import datetime
import os
import argparse # Para manejar argumentos de línea de comandos
from log_config import get_logger # Asegúrate que log_config.py esté accesible

logger = get_logger()

# Archivos por defecto (pueden ser sobrescritos por argumentos)
DEFAULT_FULL_PRODUCTS_FILE = 'wayakit_products.csv'
DEFAULT_TIMESTAMP_FILE = 'logs/last_full_run.txt'
DEFAULT_OUTPUT_FILE = 'wayakit_new_products_temp.csv'

def read_last_full_run_timestamp(filepath):
    """Lee el timestamp de la última ejecución completa."""
    try:
        with open(filepath, 'r') as f:
            timestamp_str = f.read().strip()
        # Odoo devuelve fechas UTC, asumimos que el timestamp guardado también lo es
        # Formato esperado: YYYY-MM-DD HH:MM:SS
        ts = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        logger.info(f"Timestamp de última ejecución completa leído: {ts}")
        return ts
    except FileNotFoundError:
        logger.error(f"Archivo de timestamp '{filepath}' no encontrado. No se pueden filtrar nuevos productos.")
        return None
    except Exception as e:
        logger.error(f"Error leyendo timestamp desde '{filepath}': {e}", exc_info=True)
        return None

def filter_new_products(full_products_file, timestamp_file, output_file):
    """Filtra productos basados en la fecha de creación y el timestamp."""
    logger.info(f"Iniciando filtro de productos nuevos.")
    logger.info(f"Leyendo productos completos desde: {full_products_file}")
    logger.info(f"Usando timestamp desde: {timestamp_file}")
    logger.info(f"Archivo de salida para nuevos productos: {output_file}")

    last_run_ts = read_last_full_run_timestamp(timestamp_file)
    if last_run_ts is None:
        logger.error("No se pudo obtener el timestamp. Saliendo del script de filtrado.")
        # Escribir un archivo vacío para evitar errores posteriores? O manejarlo en el bash script.
        # Por ahora, salimos. El script bash debería detectar el fallo.
        exit(1) # Salir con error

    try:
        # Leer el CSV completo, asegurándose de parsear la columna create_date
        # Odoo guarda fechas como strings UTC 'YYYY-MM-DD HH:MM:SS'
        df_all = pd.read_csv(full_products_file, parse_dates=['create_date'])
        logger.info(f"Leídos {len(df_all)} productos desde {full_products_file}.")
    except FileNotFoundError:
        logger.error(f"Archivo de productos completo '{full_products_file}' no encontrado.")
        exit(1)
    except KeyError:
        logger.error(f"La columna 'create_date' no se encontró en '{full_products_file}'. ¿Ejecutaste 'odoo_api_products.py' modificado?")
        exit(1)
    except Exception as e:
        logger.error(f"Error leyendo '{full_products_file}': {e}", exc_info=True)
        exit(1)

    # Filtrar productos cuya fecha de creación es posterior al último timestamp
    # Es importante que ambas fechas estén en la misma zona horaria (o sin zona horaria) para comparar
    # Pandas por defecto parsea sin zona horaria, lo cual debería funcionar si el timestamp también lo está.
    df_new = df_all[df_all['create_date'] > last_run_ts].copy()

    logger.info(f"Se encontraron {len(df_new)} productos creados después de {last_run_ts}.")

    try:
        # Guardar solo los productos nuevos
        df_new.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Archivo con productos nuevos guardado en: '{output_file}'")
    except Exception as e:
        logger.error(f"Error guardando archivo filtrado '{output_file}': {e}", exc_info=True)
        exit(1)

if __name__ == "__main__":
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description="Filtra productos de Wayakit basados en la fecha de la última ejecución completa.")
    parser.add_argument('--input', default=DEFAULT_FULL_PRODUCTS_FILE, help=f"Archivo CSV con todos los productos (default: {DEFAULT_FULL_PRODUCTS_FILE})")
    parser.add_argument('--timestamp', default=DEFAULT_TIMESTAMP_FILE, help=f"Archivo con el timestamp de la última ejecución completa (default: {DEFAULT_TIMESTAMP_FILE})")
    parser.add_argument('--output', default=DEFAULT_OUTPUT_FILE, help=f"Archivo CSV donde guardar los productos filtrados (default: {DEFAULT_OUTPUT_FILE})")
    args = parser.parse_args()

    filter_new_products(args.input, args.timestamp, args.output)