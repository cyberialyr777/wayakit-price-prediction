import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score
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
    
    logger.info("[OK] Files loaded.")
    return df_vol_comp, df_unit_comp

def apply_sanity_filters(df, metric_col):
    """
    Applies hard sanity filters to remove clearly corrupted data.
    """
    logger.info(f"--- 2b. Applying sanity filters for '{metric_col}' ---")
    original_rows = len(df)
    if df.empty:
        return df

    if metric_col == 'price_per_liter':
        # Remove products with suspiciously low volume (< 10ml)
        df = df[df['volume_liters'] >= 0.01].copy()
        # Cap price_per_liter at 500 MXN/L - even premium Mexican brands dont exceed this
        # This removes inflated Amazon imports ($197 for 125ml = 1,576 MXN/L)
        df = df[df['price_per_liter'] <= 500].copy()
    elif metric_col == 'price_per_item':
        # Remove absurd price_per_item
        df = df[df['price_per_item'] <= 50000].copy()

    removed = original_rows - len(df)
    if removed > 0:
        logger.warning(f"Sanity filter removed {removed} rows with corrupted data.")
    return df

def remove_duplicates(df, key_cols):
    """
    Removes duplicate rows based on key columns.
    """
    original_rows = len(df)
    df = df.drop_duplicates(subset=key_cols).copy()
    removed = original_rows - len(df)
    if removed > 0:
        logger.info(f"Removed {removed} duplicate rows.")
    return df

def filter_outliers_iqr(df_comp, metric_col, k=1.5):
    """
    Filters competitor data to remove outliers using IQR method per product type.
    More robust than percentile-based filtering for small datasets.
    """
    logger.info(f"--- 3. Filtering outliers for '{metric_col}' using IQR (k={k}) ---")
    if df_comp.empty:
        return df_comp

    original_rows = len(df_comp)
    cleaned_frames = []
    
    for product_type, group in df_comp.groupby('type_of_product'):
        if len(group) < 4:
            logger.warning(f"  '{product_type}': only {len(group)} rows, skipping IQR filter.")
            cleaned_frames.append(group)
            continue
        
        Q1 = group[metric_col].quantile(0.25)
        Q3 = group[metric_col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - k * IQR
        upper = Q3 + k * IQR
        filtered = group[(group[metric_col] >= lower) & (group[metric_col] <= upper)]
        removed = len(group) - len(filtered)
        if removed > 0:
            logger.info(f"  '{product_type}': removed {removed} outliers (range: {lower:.2f} - {upper:.2f})")
        cleaned_frames.append(filtered)
    
    df_cleaned = pd.concat(cleaned_frames) if cleaned_frames else df_comp.iloc[0:0]
    logger.info(f"Rows before: {original_rows} | Rows after: {len(df_cleaned)}")
    return df_cleaned

def save_market_stats(df_vol, df_unit, model_dir):
    """
    Calcula y guarda estadísticas de mercado (Min, Max, Median, Conteo) por tipo de producto.
    The median is used as a fallback when model confidence is low.
    """
    logger.info("--- 5a. Saving Market Stats ---")
    
    # 1. Estadísticas Volumétricas (Precio por Litro)
    if not df_vol.empty:
        vol_stats = df_vol.groupby('type_of_product')['price_per_liter'].agg(
            market_min='min', 
            market_max='max',
            market_median='median',
            competitor_count='count'
        ).reset_index()
    else:
        vol_stats = pd.DataFrame(columns=['type_of_product', 'market_min', 'market_max', 'market_median', 'competitor_count'])
    
    # 2. Estadísticas Unitarias (Precio por Pieza)
    if not df_unit.empty:
        unit_stats = df_unit.groupby('type_of_product')['price_per_item'].agg(
            market_min='min', 
            market_max='max',
            market_median='median',
            competitor_count='count'
        ).reset_index()
    else:
        unit_stats = pd.DataFrame(columns=['type_of_product', 'market_min', 'market_max', 'market_median', 'competitor_count'])

    # Guardamos estos "diccionarios" en disco
    joblib.dump(vol_stats, os.path.join(model_dir, 'vol_market_stats.joblib'))
    joblib.dump(unit_stats, os.path.join(model_dir, 'unit_market_stats.joblib'))
    
    logger.info("[OK] Market stats saved (Min, Max, Median, Count per product type).")

def main():
    """
    Orchestrates the entire training process: loading, preparation,
    training and saving models.
    """
    try:
        # Data loading
        df_vol_comp, df_unit_comp = load_training_data()

        # --- Data cleaning pipeline ---
        # Step 2b: Sanity filters (remove clearly corrupted data)
        df_vol_comp = apply_sanity_filters(df_vol_comp, 'price_per_liter')
        df_unit_comp = apply_sanity_filters(df_unit_comp, 'price_per_item')

        # Step 2c: Remove duplicates
        logger.info("--- 2c. Removing duplicates ---")
        df_vol_comp = remove_duplicates(df_vol_comp, ['company', 'volume_liters', 'price_per_liter', 'type_of_product'])
        df_unit_comp = remove_duplicates(df_unit_comp, ['company', 'total_quantity', 'price_per_item', 'type_of_product'])

        # Step 3: Filter outliers with IQR
        df_vol_comp_cleaned = filter_outliers_iqr(df_vol_comp, 'price_per_liter')
        df_unit_comp_cleaned = filter_outliers_iqr(df_unit_comp, 'price_per_item')

        # --- Data transformation ---
        logger.info("--- 4. Preparing data for training ---")
        
        # Columns needed for training (include source for weighting)
        cols_vol = ['subindustry', 'type_of_product', 'channel', 'volume_liters', 'price_per_liter', 'source']
        cols_unit = ['subindustry', 'type_of_product', 'channel', 'total_quantity', 'price_per_item', 'source']

        # Only keep columns that exist in the cleaned data
        cols_vol = [c for c in cols_vol if c in df_vol_comp_cleaned.columns]
        cols_unit = [c for c in cols_unit if c in df_unit_comp_cleaned.columns]

        df_train_vol = df_vol_comp_cleaned[cols_vol].copy()
        df_train_unit = df_unit_comp_cleaned[cols_unit].copy()

        # Add log(volume) feature to capture economies of scale
        if not df_train_vol.empty:
            df_train_vol['log_volume'] = np.log1p(df_train_vol['volume_liters'])
        if not df_train_unit.empty:
            df_train_unit['log_quantity'] = np.log1p(df_train_unit['total_quantity'])
        
        logger.info(f"Total data for volumetric training: {len(df_train_vol)} rows.")
        logger.info(f"Total data for unit training: {len(df_train_unit)} rows.")

        # --- Calculate sample weights by source ---
        # Local Mexican stores (Chedraui, Soriana) get higher weight = more realistic prices
        SOURCE_WEIGHTS = {
            'chedraui': 3.0,
            'soriana': 3.0,
            'petco': 2.0,
            'walmart': 2.0,
            'amazon': 1.0,
        }
        DEFAULT_WEIGHT = 1.0

        def get_sample_weights(df):
            if 'source' in df.columns:
                weights = df['source'].map(SOURCE_WEIGHTS).fillna(DEFAULT_WEIGHT)
                for src in df['source'].unique():
                    w = SOURCE_WEIGHTS.get(src, DEFAULT_WEIGHT)
                    cnt = (df['source'] == src).sum()
                    logger.info(f"    Source '{src}': {cnt} rows, weight={w}x")
                return weights.values
            return None

        # --- Model training and saving ---
        logger.info("--- 5. Training and saving models ---")
        
        model_dir = 'ml_model/trained_models'
        os.makedirs(model_dir, exist_ok=True)

        # Volumetric model: use log_volume instead of raw volume
        features_vol = ['log_volume', 'type_of_product', 'subindustry', 'channel']
        X_vol_encoded = pd.get_dummies(df_train_vol[features_vol].fillna('Desconocido'), columns=['type_of_product', 'subindustry', 'channel'], drop_first=True)
        
        if not df_train_vol.empty:
            y_vol = df_train_vol['price_per_liter']
            sample_weights_vol = get_sample_weights(df_train_vol)
            
            model_vol = RandomForestRegressor(n_estimators=100, random_state=42, min_samples_leaf=2, oob_score=True)
            model_vol.fit(X_vol_encoded, y_vol, sample_weight=sample_weights_vol)
            
            # Report model quality metrics
            logger.info(f"  [METRIC] Volumetric Model OOB R2 Score: {model_vol.oob_score_:.4f}")
            if len(X_vol_encoded) >= 5:
                cv_scores = cross_val_score(model_vol, X_vol_encoded, y_vol, cv=min(5, len(X_vol_encoded)), scoring='neg_mean_absolute_error')
                logger.info(f"  [METRIC] Volumetric Model MAE (5-fold CV): {-cv_scores.mean():.2f} +/- {cv_scores.std():.2f}")
            
            joblib.dump(model_vol, os.path.join(model_dir, 'volumetric_model.joblib'))
            joblib.dump(X_vol_encoded.columns.tolist(), os.path.join(model_dir, 'volumetric_model_columns.joblib'))
        else:
            logger.warning("No volumetric data to train model.")

        # Unit model: use log_quantity instead of raw quantity
        if not df_train_unit.empty:
            features_unit = ['log_quantity', 'type_of_product', 'subindustry', 'channel']
            X_unit_encoded = pd.get_dummies(df_train_unit[features_unit].fillna('Desconocido'), columns=['type_of_product', 'subindustry', 'channel'], drop_first=True)
            y_unit = df_train_unit['price_per_item']
            sample_weights_unit = get_sample_weights(df_train_unit)
            model_unit = RandomForestRegressor(n_estimators=100, random_state=42, min_samples_leaf=2, oob_score=True)
            model_unit.fit(X_unit_encoded, y_unit, sample_weight=sample_weights_unit)
            
            # Report model quality metrics
            logger.info(f"  [METRIC] Unit Model OOB R2 Score: {model_unit.oob_score_:.4f}")
            if len(X_unit_encoded) >= 5:
                cv_scores = cross_val_score(model_unit, X_unit_encoded, y_unit, cv=min(5, len(X_unit_encoded)), scoring='neg_mean_absolute_error')
                logger.info(f"  [METRIC] Unit Model MAE (5-fold CV): {-cv_scores.mean():.2f} +/- {cv_scores.std():.2f}")
            
            joblib.dump(model_unit, os.path.join(model_dir, 'unit_model.joblib'))
            joblib.dump(X_unit_encoded.columns.tolist(), os.path.join(model_dir, 'unit_model_columns.joblib'))
        else:
            logger.warning("No unit data to train model.")

        save_market_stats(df_train_vol, df_train_unit, model_dir)
        
        logger.info("[OK] Models and columns successfully saved.")

    except Exception as e:
        logger.error("Error occurred in training process", exc_info=True)

if __name__ == "__main__":
    main()