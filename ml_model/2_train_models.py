import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
import joblib
import os
import sys

# Añadir el directorio raíz al path para poder importar log_config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from log_config import get_logger

logger = get_logger()

def load_training_data():
    """Loads all CSV files necessary for training."""
    logger.info("--- 1. Loading base files ---")
    df_quotes_raw = pd.read_csv('ml_model/wayakit_cotizations.csv')
    df_wayakit_products = pd.read_csv('ml_model/wayakit_products_to_predict_odoo.csv')
    df_vol_comp = pd.read_csv('ml_model/competitor_volumetric_processed.csv')
    df_unit_comp = pd.read_csv('ml_model/competitor_unit_processed.csv')
    logger.info("✅ Files loaded.")
    return df_quotes_raw, df_wayakit_products, df_vol_comp, df_unit_comp

def prepare_wayakit_training_data(df_quotes_raw, df_wayakit_products):
    """Prepares Wayakit's historical quotation data."""
    logger.info("--- 2. Preparing Wayakit quotation data ---")
    df_quotes_raw.columns = df_quotes_raw.columns.str.strip()
    df_wayakit_products.columns = df_wayakit_products.columns.str.strip()
    
    df_wayakit_products_renamed = df_wayakit_products.rename(columns={
        'Industry': 'industry', 'SubIndustry': 'subindustry',
        'Generic product type': 'generic_product_type', 'Type_of_product': 'type_of_product'
    })

    df_wayakit_train_base = pd.merge(df_quotes_raw, df_wayakit_products_renamed, on='Product_ID', how='inner')
    df_wayakit_train_base['company'] = 'Wayakit'
    
    b2c_conditions = ['Home', 'Automotive', 'Pets']
    df_wayakit_train_base['channel'] = np.where(df_wayakit_train_base['subindustry'].isin(b2c_conditions), 'B2C', 'B2B')
    
    logger.info(f"✅ Prepared {len(df_wayakit_train_base)} Wayakit quotations.")
    return df_wayakit_train_base

def filter_outliers_with_percentiles(df_comp, metric_col, p_lower=0.10, p_upper=0.90):
    """
    Filters competitor data to remove outliers using percentiles.
    This is your quantile handling logic.
    """
    logger.info(f"--- 3. Filtering outliers for '{metric_col}' using percentiles {p_lower}-{p_upper} ---")
    lower_bound = df_comp.groupby('type_of_product')[metric_col].transform('quantile', p_lower)
    upper_bound = df_comp.groupby('type_of_product')[metric_col].transform('quantile', p_upper)
    
    original_rows = len(df_comp)
    df_cleaned = df_comp[(df_comp[metric_col] >= lower_bound) & (df_comp[metric_col] <= upper_bound)].copy()
    
    logger.info(f"Filas antes: {original_rows} | Filas después: {len(df_cleaned)}")
    df_cleaned['approved_quote_price'] = 0.0
    return df_cleaned

def main():
    """
    Orchestrates the entire training process: loading, preparation,
    training and saving models.
    """
    try:
        # Data loading
        df_quotes, df_products, df_vol_comp, df_unit_comp = load_training_data()

        # Wayakit data preparation
        df_wayakit_base = prepare_wayakit_training_data(df_quotes, df_products)

        # **HERE IS YOUR PERCENTILE LOGIC, INTACT**
        df_vol_comp_cleaned = filter_outliers_with_percentiles(df_vol_comp, 'price_per_liter')
        df_unit_comp_cleaned = filter_outliers_with_percentiles(df_unit_comp, 'price_per_item')

        # --- Data transformation and combination (your original logic) ---
        logger.info("--- 4. Combining Wayakit and competitor data for training ---")
        cols_vol = ['industry', 'subindustry', 'type_of_product', 'generic_product_type', 'price_sar', 'company', 'unit_of_measurement', 'channel', 'volume_liters', 'price_per_liter', 'approved_quote_price']
        cols_unit = ['industry', 'subindustry', 'type_of_product', 'generic_product_type', 'price_sar', 'company', 'unit_of_measurement', 'total_quantity', 'channel', 'price_per_item', 'approved_quote_price']

        df_wayakit_vol_train = df_wayakit_base[df_wayakit_base['Volume_Liters'] > 0].copy()
        df_wayakit_vol_train['volume_liters'] = df_wayakit_vol_train['Volume_Liters']
        df_wayakit_vol_train['price_per_liter'] = df_wayakit_vol_train['approved_quote_price'] / df_wayakit_vol_train['volume_liters']
        df_wayakit_vol_train['price_sar'] = df_wayakit_vol_train['approved_quote_price']
        df_wayakit_vol_train['unit_of_measurement'] = np.where(df_wayakit_vol_train['volume_liters'] < 1, 'ml', 'l')
        df_wayakit_vol_train_final = df_wayakit_vol_train[cols_vol].dropna(subset=['price_per_liter'])


        df_wayakit_unit_train = df_wayakit_base[df_wayakit_base['Pack_quantity_Units'] > 0].copy()
        df_wayakit_unit_train['price_per_item'] = df_wayakit_unit_train['approved_quote_price'] / df_wayakit_unit_train['Pack_quantity_Units']
        df_wayakit_unit_train['price_sar'] = df_wayakit_unit_train['approved_quote_price']
        df_wayakit_unit_train['total_quantity'] = pd.to_numeric(df_wayakit_unit_train['Pack_quantity_Units'])
        df_wayakit_unit_train['unit_of_measurement'] = 'units'
        df_wayakit_unit_train_final = df_wayakit_unit_train[cols_unit].dropna(subset=['price_per_item'])


        df_train_vol = pd.concat([df_vol_comp_cleaned[cols_vol], df_wayakit_vol_train_final], ignore_index=True)
        df_train_unit = pd.concat([df_unit_comp_cleaned[cols_unit], df_wayakit_unit_train_final], ignore_index=True)
        
        logger.info(f"Total data for volumetric training: {len(df_train_vol)} rows.")
        logger.info(f"Total data for unit training: {len(df_train_unit)} rows.")

        # --- Model training and saving (your original logic) ---
        logger.info("--- 5. Training and saving models ---")
        features_vol = ['volume_liters', 'type_of_product', 'subindustry', 'channel', 'approved_quote_price']
        X_vol_encoded = pd.get_dummies(df_train_vol[features_vol].fillna('Desconocido'), columns=['type_of_product', 'subindustry', 'channel'], drop_first=True)
        model_vol = RandomForestRegressor(n_estimators=100, random_state=42, min_samples_leaf=2, oob_score=True).fit(X_vol_encoded, df_train_vol['price_per_liter'])

        features_unit = ['total_quantity', 'type_of_product', 'subindustry', 'channel', 'approved_quote_price']
        X_unit_encoded = pd.get_dummies(df_train_unit[features_unit].fillna('Desconocido'), columns=['type_of_product', 'subindustry', 'channel'], drop_first=True)
        model_unit = RandomForestRegressor(n_estimators=100, random_state=42, min_samples_leaf=2, oob_score=True).fit(X_unit_encoded, df_train_unit['price_per_item'])

        model_dir = 'ml_model/trained_models'
        os.makedirs(model_dir, exist_ok=True)
        joblib.dump(model_vol, os.path.join(model_dir, 'volumetric_model.joblib'))
        joblib.dump(X_vol_encoded.columns.tolist(), os.path.join(model_dir, 'volumetric_model_columns.joblib'))
        joblib.dump(model_unit, os.path.join(model_dir, 'unit_model.joblib'))
        joblib.dump(X_unit_encoded.columns.tolist(), os.path.join(model_dir, 'unit_model_columns.joblib'))
        
        logger.info("🎉 Models and columns successfully saved.")

    except Exception as e:
        logger.error("Error occurred in training process", exc_info=True)

if __name__ == "__main__":
    main()