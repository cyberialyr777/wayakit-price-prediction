INSTRUCTIONS_FILE = 'scraper/analysis-odoo.csv'
OUTPUT_CSV_FILE = 'scraper/competitors_complete.csv'
CSV_COLUMNS = [
    'date', 'industry', 'subindustry', 'type_of_product', 'generic_product_type',
    'product', 'price_sar', 'company', 'source', 'url',
    'unit_of_measurement', 'total_quantity', 'channel'
]

TARGET_MAP = {
    'Home': ['amazon', 'chedraui'],
    'Pets': ['amazon', 'petco', 'chedraui'],
}

# Productos a excluir de Chedraui (usar el valor de "Type of product")
CHEDRAUI_EXCLUSIONS = ['H9-Spray para ropa']

# Precio maximo para filtrar productos en Amazon (en MXN).
# Filtra productos importados caros que no reflejan el mercado mexicano.
AMAZON_MAX_PRICE = 250

# USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"