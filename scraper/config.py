INSTRUCTIONS_FILE = 'analysis-odoo.csv'
OUTPUT_CSV_FILE = 'scraper/competitors_complete.csv'
CSV_COLUMNS = [
    'date', 'industry', 'subindustry', 'type_of_product', 'generic_product_type',
    'product', 'price_sar', 'company', 'source', 'url',
    'unit_of_measurement', 'total_quantity', 'channel'
]

TARGET_MAP = {
    # 'Home': ['amazon', 'mumzworld', 'saco'],
    # 'Automotive': ['amazon', 'saco'],
    'Pets': ['amazon'],
    # 'Aviation': ['aerosense'],
    # 'Airports': ['fine'],
    # 'Restaurants': ['fine'],
    # 'Facilities management': ['fine', 'gogreen', 'officesupply'],
    # 'Faith': ['fine', 'gogreen', 'officesupply'],
    # 'Gyms': ['fine', 'gogreen', 'officesupply'],
    # 'Land Transportation': ['fine', 'gogreen'],
    # 'Spas and salons': ['fine', 'gogreen', 'officesupply'],
    # 'Hotels': ['fine', 'gogreen', 'officesupply'],
    # 'Healthcare': ['fine', 'saco'],
    # 'Industrial facilities': ['fine', 'gogreen', 'officesupply'],
}

MUMZWORLD_EXCLUSIONS = [
    'oven and grill cleaner',
    'shower and tub cleaner',
    'mold and mildew remover',
    'general sanitizer for vegetable and salad washing',
    'tile and laminate cleaner',
    'wax and floor polish',
    'carpet shampoo',
    'spot remover for carpets',
    'leather cleaner',
]

SACO_EXCLUSIONS = [
    'microfiber for vehicle cleaning',
    'long brush for seating cleaning',
    'general sanitizer for vegetable and salad washing',
    'fabric refresher',
    'car surface disinfectant wet rags',
    'car water spot remover',
    'car bug and poop remover',
    'waterless car wash product',
    'car surface disinfectant',
    'car gum remover',
    'car air freshener',
    'broad-spectrum disinfectant for surfaces, mattress and touchpoints',
    'body wash gel',
    'liquid dish soap',
    'makeup brush cleanser',
    'high performance waterless carpet cleaner',
    'surface disinfectant wet rags',
    'restroom deodorizer',
    'degreaser for food service areas',
    'sewage odor control spray',
    'non-slip floor cleaner for safety',
    'floor degreaser for kitchen areas',
    'grease trap odor treatment',
    'stainless steel polish and cleaner for fixtures and fittings',
    'escalator cleaner',
    'escalator tread brightener',
    'epoxy cleaner for heavy-traffic parking lot area'
    'hvac system cleaner',
    'hvac system deodorizer',
    'fabric refresher for linen, towels, curtains, mattresses and upholstery',
    'hard floor polish',
    'streak-free glass cleaner for large window areas'
]

# USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"