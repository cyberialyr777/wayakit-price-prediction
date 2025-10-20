# scraper/log_config.py
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs') # Directorio logs en la raíz del proyecto
LOG_FILENAME = os.path.join(LOG_DIR, 'wayakit_app.log')

def setup_logging(log_level=logging.INFO):
    """Configura el logging para la aplicación."""

    # Crear directorio de logs si no existe
    os.makedirs(LOG_DIR, exist_ok=True)

    # Crear el logger principal
    logger = logging.getLogger('wayakit_price_prediction') # Nombre único para tu logger
    logger.setLevel(log_level) # Nivel mínimo de severidad a registrar

    # Evitar duplicar handlers si la función se llama múltiples veces
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formato de los mensajes de log
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 1. Handler para escribir en archivo (con rotación)
    # Rotará el archivo cuando alcance 5MB, manteniendo 3 archivos de respaldo.
    file_handler = RotatingFileHandler(
        LOG_FILENAME, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # 2. Handler para escribir en la consola (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # Configurar logging para librerías de terceros (opcional, para reducir ruido)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('webdriver_manager').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    return logger

# Configurar el logger al importar el módulo
logger = setup_logging()

# Puedes añadir una función simple para obtener el logger configurado desde otros módulos
def get_logger():
  """Retorna la instancia del logger configurado."""
  return logging.getLogger('wayakit_price_prediction')