import pandas as pd
import numpy as np
import joblib
import os

def load_artifacts():
    """Carga los modelos entrenados y las listas de columnas desde el disco."""
    print("--- 1. Cargando modelos y columnas entrenadas ---")
    model_dir = 'ml_model/trained_models'
    
    try:
        model_vol = joblib.load(os.path.join(model_dir, 'volumetric_model.joblib'))
        model_unit = joblib.load(os.path.join(model_dir, 'unit_model.joblib'))
        vol_cols = joblib.load(os.path.join(model_dir, 'volumetric_model_columns.joblib'))
        unit_cols = joblib.load(os.path.join(model_dir, 'unit_model_columns.joblib'))
        
        print("✅ Modelos y columnas cargados exitosamente.")
        return model_vol, model_unit, vol_cols, unit_cols
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontraron los archivos de modelo en la carpeta '{model_dir}'.")
        print("➡️ Solución: Asegúrate de haber ejecutado '2_train_models.py' primero.")
        return None, None, None, None

def prepare_prediction_data():
    """Carga los productos de Wayakit, los limpia y los enriquece con datos de cotizaciones pasadas."""
    print("\n--- 2. Preparando la lista de productos de Wayakit para la predicción ---")
    df_wayakit = pd.read_csv('wayakit_products_to_predict_odoo.csv')
    df_quotes_raw = pd.read_csv('wayakit_cotizations.csv')
    
    # --- CORRECCIÓN DE ORDEN (COMO SUGERISTE) ---
    # 1. Limpiar y renombrar columnas ANTES de unir.
    df_wayakit.columns = df_wayakit.columns.str.strip()
    df_wayakit.rename(columns={
        'SubIndustry': 'subindustry', 'Industry': 'industry', 
        'Generic product type': 'generic_product_type', 'Type_of_product':'type_of_product'
    }, inplace=True)

    # 2. Preparar y unir con cotizaciones.
    quotes_df = df_quotes_raw[['Product_ID', 'approved_quote_price']].drop_duplicates(subset='Product_ID', keep='last')
    df_wayakit = pd.merge(df_wayakit, quotes_df, on='Product_ID', how='left')
    df_wayakit['approved_quote_price'] = df_wayakit['approved_quote_price'].fillna(0.0)

    # 3. Añadir columnas 'company' y 'channel'.
    df_wayakit['company'] = 'Wayakit'
    b2c_subindustries = ['home', 'automotive', 'pets']
    df_wayakit['channel'] = np.where(df_wayakit['subindustry'].str.lower().isin(b2c_subindustries), 'B2C', 'B2B')
    
    print(f"✅ Se prepararon {len(df_wayakit)} productos para la predicción.")
    return df_wayakit

def generate_predictions(df_wayakit, model_vol, model_unit, vol_cols, unit_cols):
    """Itera sobre los productos y genera una predicción de precio para cada uno."""
    print("\n--- 3. Iniciando la generación de predicciones ---")
    report_list = []

    for _, row in df_wayakit.iterrows():
        # Tu lógica de predicción y regla de negocio (intacta)
        is_volumetric = pd.notna(row['Volume_Liters']) and row['Volume_Liters'] > 0
        is_unit = pd.notna(row['Pack_quantity_Units']) and row['Pack_quantity_Units'] > 0
        
        single_product_df = pd.DataFrame([row])
        model_predicted_price = 0
        predicted_price_per_unit = 0

        if is_volumetric:
            single_product_df = single_product_df.rename(columns={'Volume_Liters': 'volume_liters'})
            features_to_encode = ['volume_liters', 'type_of_product', 'subindustry', 'company', 'channel', 'approved_quote_price']
            X_pred_encoded = pd.get_dummies(single_product_df[features_to_encode], columns=['type_of_product', 'subindustry', 'company', 'channel'])
            X_pred_aligned = X_pred_encoded.reindex(columns=vol_cols, fill_value=0)
            predicted_price_per_unit = model_vol.predict(X_pred_aligned)[0]
            model_predicted_price = predicted_price_per_unit * row['Volume_Liters']
        
        elif is_unit:
            single_product_df = single_product_df.rename(columns={'Pack_quantity_Units': 'total_quantity'})
            features_to_encode = ['total_quantity', 'type_of_product', 'subindustry', 'company', 'channel', 'approved_quote_price']
            X_pred_encoded = pd.get_dummies(single_product_df[features_to_encode], columns=['type_of_product', 'subindustry', 'company', 'channel'])
            X_pred_aligned = X_pred_encoded.reindex(columns=unit_cols, fill_value=0)
            predicted_price_per_unit = model_unit.predict(X_pred_aligned)[0]
            model_predicted_price = predicted_price_per_unit * row['Pack_quantity_Units']

        cost = row['Unit_cost_SAR']
        min_profitable_price = cost * 1.30
        final_price = max(model_predicted_price, min_profitable_price)
        profit_margin = ((final_price - cost) / cost) * 100 if cost > 0 else 0

        report_list.append({
            'Product_ID': row['Product_ID'], 'product_name': row['Product_Name'],
            'product_type': row.get('type_of_product', 'N/A'), 'generic_product_type': row.get('generic_product_type', 'N/A'),
            'subindustry': row.get('subindustry', 'N/A'), 'industry': row.get('industry', 'N/A'),
            'volume': f"{row['Volume_Liters']} L" if is_volumetric else f"{int(row.get('Pack_quantity_Units', 0))} units",
            'cost_per_unit': cost, 'predicted_price': round(final_price, 2),
            'predicted_price_per_unit': round(predicted_price_per_unit, 2), 'porcentaje_de_ganancia': round(profit_margin, 2)
        })
        
    return pd.DataFrame(report_list)

def main():
    """Orquesta el proceso completo de predicción y generación de reporte."""
    print("\n" + "="*60)
    print("--- Iniciando Script de Predicción de Precios ---")
    print("="*60)
    
    artifacts = load_artifacts()
    if not all(artifacts): # Si falla la carga de modelos, no continuar
        return
        
    model_vol, model_unit, vol_cols, unit_cols = artifacts
    
    df_to_predict = prepare_prediction_data()
    report_df = generate_predictions(df_to_predict, model_vol, model_unit, vol_cols, unit_cols)

    # Guardar y analizar el reporte final
    output_filename = 'wayakit_prediction_report.csv'
    report_df.to_csv(output_filename, index=False)
    print(f"\n--- 4. Reporte final guardado como '{output_filename}' ---")

    loss_products_count = (report_df['predicted_price'] <= report_df['cost_per_unit']).sum()
    print("\n" + "-"*50)
    print("Análisis de Rentabilidad:")
    print(f"Se encontraron {loss_products_count} productos cuyo precio de venta sugerido es MENOR o IGUAL a su costo.")
    print("-" * 50)
    
    print("\n🎉 Proceso de predicción completado exitosamente.")

if __name__ == "__main__":
    main()