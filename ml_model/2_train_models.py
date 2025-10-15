import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
import joblib
import os

def load_training_data():
    """Carga todos los archivos CSV necesarios para el entrenamiento."""
    print("--- 1. Cargando archivos base ---")
    df_quotes_raw = pd.read_csv('wayakit_cotizations.csv')
    df_wayakit_products = pd.read_csv('wayakit_products_to_predict_odoo.csv')
    df_vol_comp = pd.read_csv('competitor_volumetric_processed.csv')
    df_unit_comp = pd.read_csv('competitor_unit_processed.csv')
    print("✅ Archivos cargados.")
    return df_quotes_raw, df_wayakit_products, df_vol_comp, df_unit_comp

def prepare_wayakit_training_data(df_quotes_raw, df_wayakit_products):
    """Prepara los datos históricos de cotizaciones de Wayakit."""
    print("\n--- 2. Preparando datos de cotizaciones de Wayakit ---")
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
    
    print(f"✅ Se prepararon {len(df_wayakit_train_base)} cotizaciones de Wayakit.")
    return df_wayakit_train_base

def filter_outliers_with_percentiles(df_comp, metric_col, p_lower=0.10, p_upper=0.90):
    """
    Filtra los datos de la competencia para eliminar outliers usando percentiles.
    Esta es tu lógica de manejo de cuantiles.
    """
    print(f"\n--- 3. Filtrando outliers para '{metric_col}' usando percentiles {p_lower}-{p_upper} ---")
    lower_bound = df_comp.groupby('type_of_product')[metric_col].transform('quantile', p_lower)
    upper_bound = df_comp.groupby('type_of_product')[metric_col].transform('quantile', p_upper)
    
    original_rows = len(df_comp)
    df_cleaned = df_comp[(df_comp[metric_col] >= lower_bound) & (df_comp[metric_col] <= upper_bound)].copy()
    
    print(f"Filas antes: {original_rows} | Filas después: {len(df_cleaned)}")
    df_cleaned['approved_quote_price'] = 0.0
    return df_cleaned

def main():
    """
    Orquesta todo el proceso de entrenamiento: carga, preparación,
    entrenamiento y guardado de modelos.
    """
    try:
        # Carga de datos
        df_quotes, df_products, df_vol_comp, df_unit_comp = load_training_data()

        # Preparación de datos de Wayakit
        df_wayakit_base = prepare_wayakit_training_data(df_quotes, df_products)

        # **AQUÍ ESTÁ TU LÓGICA DE PERCENTILES, INTACTA**
        df_vol_comp_cleaned = filter_outliers_with_percentiles(df_vol_comp, 'price_per_liter')
        df_unit_comp_cleaned = filter_outliers_with_percentiles(df_unit_comp, 'price_per_item')

        # --- Transformación y combinación de datos (tu lógica original) ---
        print("\n--- 4. Combinando datos de Wayakit y competencia para el entrenamiento ---")
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
        
        print(f"Total datos para entrenamiento volumétrico: {len(df_train_vol)} filas.")
        print(f"Total datos para entrenamiento de unidades: {len(df_train_unit)} filas.")

        # --- Entrenamiento y guardado de modelos (tu lógica original) ---
        print("\n--- 5. Entrenando y guardando modelos ---")
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
        
        print("\n🎉 Modelos y columnas guardados exitosamente.")

    except Exception as e:
        print(f"❌ Ocurrió un error en el proceso de entrenamiento: {e}")

if __name__ == "__main__":
    main()