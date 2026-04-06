import pandas as pd
import numpy as np
import os
import sys
# Add directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scraper'))
from log_config import get_logger
try:
    import config
except ImportError:
    config = None

logger = get_logger()

def load_and_clean_raw_data():
    logger.info("--- 1. Loading and cleaning raw competitor data ---")
    df_test4 = pd.read_csv('scraper/competitors_complete.csv')
    
    # Keep source column for weighting in the model
    df_test4_clean = df_test4[['industry', 'subindustry', 'type_of_product', 'generic_product_type', 'price_sar', 'company', 'unit_of_measurement', 'total_quantity', 'channel', 'source']].copy()

    def clean_price(price_column):
        return pd.to_numeric(price_column.astype(str).str.replace('SAR', '').str.replace(',', '').str.strip(), errors='coerce')
    df_test4_clean['price_sar'] = clean_price(df_test4_clean['price_sar'])

    def clean_units(unit_column):
        return unit_column.str.lower().str.strip()
    df_test4_clean['unit_of_measurement'] = clean_units(df_test4_clean['unit_of_measurement'])
    
    # Filter Amazon products by max price to remove expensive imports
    amazon_max = getattr(config, 'AMAZON_MAX_PRICE', None)
    if amazon_max is not None:
        before = len(df_test4_clean)
        df_test4_clean = df_test4_clean[
            ~((df_test4_clean['source'] == 'amazon') & (df_test4_clean['price_sar'] > amazon_max))
        ].copy()
        removed = before - len(df_test4_clean)
        if removed > 0:
            logger.info(f"Filtered {removed} Amazon products above ${amazon_max} MXN.")
    
    competitor_data = df_test4_clean
    logger.info(f"[OK] Raw data loaded. Total: {len(competitor_data)} rows.")
    return competitor_data

def process_volumetric_data(df):
    logger.info("--- 2a. Processing volumetric products ---")
    df['unit_of_measurement'] = df['unit_of_measurement'].str.lower().str.strip()
    
    original_rows = len(df)
    df = df[df['unit_of_measurement'] != 'g']
    logger.info(f"Excluded {original_rows - len(df)} products measured in grams.")

    def convert_to_liters(row):
        if row['unit_of_measurement'] == 'ml': return row['total_quantity'] / 1000
        if row['unit_of_measurement'] == 'fl oz': return row['total_quantity'] * 0.0295735
        if row['unit_of_measurement'] == 'l': return row['total_quantity']
        return None
    df['volume_liters'] = df.apply(convert_to_liters, axis=1)

    df.dropna(subset=['volume_liters'], inplace=True)
    df['price_per_liter'] = df['price_sar'] / df['volume_liters']
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(subset=['price_per_liter'], inplace=True)
    
    df.to_csv('ml_model/competitor_volumetric_processed.csv', index=False)
    logger.info("[OK] File 'competitor_volumetric_processed.csv' saved.")

def process_unit_data(df):
    logger.info("--- 2b. Processing unit products ---")
    df['total_quantity'] = pd.to_numeric(df['total_quantity'], errors='coerce')
    df.dropna(subset=['total_quantity'], inplace=True)
    df = df[df['total_quantity'] > 0]
    
    df['price_per_item'] = df['price_sar'] / df['total_quantity']
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(subset=['price_per_item'], inplace=True)
    
    df.to_csv('ml_model/competitor_unit_processed.csv', index=False)
    logger.info("[OK] File 'competitor_unit_processed.csv' saved.")

def main():
    try:
        competitor_data = load_and_clean_raw_data()
        
        volumetric_units = ['ml', 'l', 'fl oz', 'g']
        is_volumetric = competitor_data['unit_of_measurement'].str.lower().isin(volumetric_units)
        
        process_volumetric_data(competitor_data[is_volumetric].copy())
        process_unit_data(competitor_data[~is_volumetric].copy())
        
        logger.info("🎉 Competitor data preprocessing completed.")
        
    except FileNotFoundError as e:
        logger.error("Required file not found", exc_info=True)
    except Exception as e:
        logger.error("Unexpected error occurred during preprocessing", exc_info=True)

if __name__ == "__main__":
    main()