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
    # Only load competitor data
    df_vol_comp = pd.read_csv('ml_model/competitor_volumetric_processed.csv')
    df_unit_comp = pd.read_csv('ml_model/competitor_unit_processed.csv')
    
    # Ensure numeric types
    df_vol_comp['price_per_liter'] = pd.to_numeric(df_vol_comp['price_per_liter'], errors='coerce')
    df_unit_comp['price_per_item'] = pd.to_numeric(df_unit_comp['price_per_item'], errors='coerce')
    
    logger.info("✅ Files loaded.")
    return df_vol_comp, df_unit_comp

def filter_outliers_with_percentiles(df_comp, metric_col, p_lower=0.10, p_upper=0.90):
    """
    Filters competitor data to remove outliers using percentiles.
    """
    logger.info(f"--- 3. Filtering outliers for '{metric_col}' using percentiles {p_lower}-{p_upper} ---")
    if df_comp.empty:
        return df_comp

    lower_bound = df_comp.groupby('type_of_product')[metric_col].transform('quantile', p_lower)
    upper_bound = df_comp.groupby('type_of_product')[metric_col].transform('quantile', p_upper)
    
    original_rows = len(df_comp)
    # Handle cases where transform returns NaN (e.g. too few samples)
    if lower_bound.isna().all() or upper_bound.isna().all():
         logger.warning("Not enough data for quantile filtering. Skipping filter.")
         return df_comp

    df_cleaned = df_comp[(df_comp[metric_col] >= lower_bound) & (df_comp[metric_col] <= upper_bound)].copy()
    
    logger.info(f"Filas antes: {original_rows} | Filas después: {len(df_cleaned)}")
    return df_cleaned

def save_market_stats(df_vol, df_unit, model_dir):
    """
    Calcula y guarda estadísticas de mercado (Min, Max, Conteo) por tipo de producto.
    """
    logger.info("--- 5a. Saving Market Stats ---")
    
    # 1. Estadísticas Volumétricas (Precio por Litro)
    if not df_vol.empty:
        vol_stats = df_vol.groupby('type_of_product')['price_per_liter'].agg(
            market_min='min', 
            market_max='max', 
            competitor_count='count'
        ).reset_index()
    else:
        vol_stats = pd.DataFrame(columns=['type_of_product', 'market_min', 'market_max', 'competitor_count'])
    
    # 2. Estadísticas Unitarias (Precio por Pieza)
    if not df_unit.empty:
        unit_stats = df_unit.groupby('type_of_product')['price_per_item'].agg(
            market_min='min', 
            market_max='max', 
            competitor_count='count'
        ).reset_index()
    else:
        unit_stats = pd.DataFrame(columns=['type_of_product', 'market_min', 'market_max', 'competitor_count'])

    # Guardamos estos "diccionarios" en disco
    joblib.dump(vol_stats, os.path.join(model_dir, 'vol_market_stats.joblib'))
    joblib.dump(unit_stats, os.path.join(model_dir, 'unit_market_stats.joblib'))
    
    logger.info("✅ Market stats saved (Min, Max, Count per product type).")

def main():
    """
    Orchestrates the entire training process: loading, preparation,
    training and saving models.
    """
    try:
        # Data loading
        df_vol_comp, df_unit_comp = load_training_data()

        # Filter outliers
        df_vol_comp_cleaned = filter_outliers_with_percentiles(df_vol_comp, 'price_per_liter')
        df_unit_comp_cleaned = filter_outliers_with_percentiles(df_unit_comp, 'price_per_item')

        # --- Data transformation ---
        logger.info("--- 4. Preparing data for training ---")
        
        # Columns for training (removed approved_quote_price)
        cols_vol = ['industry', 'subindustry', 'type_of_product', 'generic_product_type', 'price_sar', 'company', 'unit_of_measurement', 'channel', 'volume_liters', 'price_per_liter']
        cols_unit = ['industry', 'subindustry', 'type_of_product', 'generic_product_type', 'price_sar', 'company', 'unit_of_measurement', 'total_quantity', 'channel', 'price_per_item']

        df_train_vol = df_vol_comp_cleaned[cols_vol].copy()
        df_train_unit = df_unit_comp_cleaned[cols_unit].copy()
        
        logger.info(f"Total data for volumetric training: {len(df_train_vol)} rows.")
        logger.info(f"Total data for unit training: {len(df_train_unit)} rows.")

        # --- Model training and saving ---
        logger.info("--- 5. Training and saving models ---")
        
        model_dir = 'ml_model/trained_models'
        os.makedirs(model_dir, exist_ok=True)

        # Features (removed approved_quote_price)
        features_vol = ['volume_liters', 'type_of_product', 'subindustry', 'channel']
        X_vol_encoded = pd.get_dummies(df_train_vol[features_vol].fillna('Desconocido'), columns=['type_of_product', 'subindustry', 'channel'], drop_first=True)
        
        if not df_train_vol.empty:
            model_vol = RandomForestRegressor(n_estimators=100, random_state=42, min_samples_leaf=2, oob_score=True).fit(X_vol_encoded, df_train_vol['price_per_liter'])
            joblib.dump(model_vol, os.path.join(model_dir, 'volumetric_model.joblib'))
            joblib.dump(X_vol_encoded.columns.tolist(), os.path.join(model_dir, 'volumetric_model_columns.joblib'))
        else:
            logger.warning("No volumetric data to train model.")
            # Create dummy model or handle gracefully? For now just skip saving model or save None?
            # If we don't save, prediction script will fail.
            # Let's save a dummy model if needed, or just warn.
            # Actually, better to just not save and let prediction fail if it tries to load.
            pass

        features_unit = ['total_quantity', 'type_of_product', 'subindustry', 'channel']
        X_unit_encoded = pd.get_dummies(df_train_unit[features_unit].fillna('Desconocido'), columns=['type_of_product', 'subindustry', 'channel'], drop_first=True)
        
        if not df_train_unit.empty:
            model_unit = RandomForestRegressor(n_estimators=100, random_state=42, min_samples_leaf=2, oob_score=True).fit(X_unit_encoded, df_train_unit['price_per_item'])
            joblib.dump(model_unit, os.path.join(model_dir, 'unit_model.joblib'))
            joblib.dump(X_unit_encoded.columns.tolist(), os.path.join(model_dir, 'unit_model_columns.joblib'))
        else:
            logger.warning("No unit data to train model.")
            pass

        save_market_stats(df_train_vol, df_train_unit, model_dir)
        
        logger.info("🎉 Models and columns successfully saved.")

    except Exception as e:
        logger.error("Error occurred in training process", exc_info=True)

if __name__ == "__main__":
    main()