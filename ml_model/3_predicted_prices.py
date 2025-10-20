import pandas as pd
import numpy as np
import joblib
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from log_config import get_logger

logger = get_logger()

def load_artifacts():
    """Loads trained models and column lists from disk."""
    logger.info("--- 1. Loading trained models and columns ---")
    model_dir = 'ml_model/trained_models'
    
    try:
        model_vol = joblib.load(os.path.join(model_dir, 'volumetric_model.joblib'))
        model_unit = joblib.load(os.path.join(model_dir, 'unit_model.joblib'))
        vol_cols = joblib.load(os.path.join(model_dir, 'volumetric_model_columns.joblib'))
        unit_cols = joblib.load(os.path.join(model_dir, 'unit_model_columns.joblib'))
        
        logger.info("✅ Models and columns loaded successfully.")
        return model_vol, model_unit, vol_cols, unit_cols
    except FileNotFoundError:
        logger.error(f"❌ ERROR: Model files not found in folder '{model_dir}'.")
        logger.warning("➡️ Solution: Make sure you have run '2_train_models.py' first.")
        return None, None, None, None

def prepare_prediction_data():
    """Loads Wayakit products, cleans them and enriches with past quotation data."""
    logger.info("\n--- 2. Preparing Wayakit product list for prediction ---")
    df_wayakit = pd.read_csv('wayakit_products_to_predict_odoo.csv')
    df_quotes_raw = pd.read_csv('wayakit_cotizations.csv')
    
    # --- ORDER CORRECTION (AS YOU SUGGESTED) ---
    # 1. Clean and rename columns BEFORE merging.
    df_wayakit.columns = df_wayakit.columns.str.strip()
    df_wayakit.rename(columns={
        'SubIndustry': 'subindustry', 'Industry': 'industry', 
        'Generic product type': 'generic_product_type', 'Type_of_product':'type_of_product'
    }, inplace=True)

    # 2. Prepare and merge with quotations.
    quotes_df = df_quotes_raw[['Product_ID', 'approved_quote_price']].drop_duplicates(subset='Product_ID', keep='last')
    df_wayakit = pd.merge(df_wayakit, quotes_df, on='Product_ID', how='left')
    df_wayakit['approved_quote_price'] = df_wayakit['approved_quote_price'].fillna(0.0)

    # 3. Add 'company' and 'channel' columns.
    df_wayakit['company'] = 'Wayakit'
    b2c_subindustries = ['home', 'automotive', 'pets']
    df_wayakit['channel'] = np.where(df_wayakit['subindustry'].str.lower().isin(b2c_subindustries), 'B2C', 'B2B')
    
    logger.info(f"✅ Prepared {len(df_wayakit)} products for prediction.")
    return df_wayakit

def generate_predictions(df_wayakit, model_vol, model_unit, vol_cols, unit_cols):
    """Iterates over products and generates a price prediction for each one."""
    logger.info("\n--- 3. Starting prediction generation ---")
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
    """Orchestrates the complete prediction and report generation process."""
    logger.info("\n" + "="*60)
    logger.info("--- Starting Price Prediction Script ---")
    logger.info("="*60)
    
    artifacts = load_artifacts()
    if not all(artifacts): # If model loading fails, don't continue
        return
        
    model_vol, model_unit, vol_cols, unit_cols = artifacts
    
    df_to_predict = prepare_prediction_data()
    report_df = generate_predictions(df_to_predict, model_vol, model_unit, vol_cols, unit_cols)

    # Save and analyze final report
    output_filename = 'wayakit_prediction_report.csv'
    report_df.to_csv(output_filename, index=False)
    logger.info(f"\n--- 4. Final report saved as '{output_filename}' ---")

    loss_products_count = (report_df['predicted_price'] <= report_df['cost_per_unit']).sum()
    logger.info("\n" + "-"*50)
    logger.info("Profitability Analysis:")
    logger.warning(f"Found {loss_products_count} products whose suggested selling price is LOWER or EQUAL to their cost.")
    logger.info("-" * 50)
    
    logger.info("\n🎉 Prediction process completed successfully.")

if __name__ == "__main__":
    main()