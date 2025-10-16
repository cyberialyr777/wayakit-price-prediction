import pandas as pd
import numpy as np

def load_and_clean_raw_data():
    print("--- 1. Cargando y limpiando datos crudos de la competencia ---")
    df_test4 = pd.read_csv('../scraper/competitors_complete.csv')
    df_prod = pd.read_csv('wayakit_products_competition.csv')

    df_test4_clean = df_test4[['industry', 'subindustry', 'type_of_product', 'generic_product_type', 'price_sar', 'company', 'unit_of_measurement', 'total_quantity', 'channel']].copy()
    df_prod_clean = df_prod.rename(columns={
        'Industry': 'industry', 'Sub industry': 'subindustry', 'Type of product': 'type_of_product',
        'Generic product type': 'generic_product_type', 'Price per unit SAR': 'price_sar',
        'Company': 'company', 'Unit of measurement [mL,g,units]': 'unit_of_measurement',
        'Total quantity': 'total_quantity', 'Channel': 'channel'
    })
    df_prod_clean = df_prod_clean[df_test4_clean.columns]

    def clean_price(price_column):
        return pd.to_numeric(price_column.astype(str).str.replace('SAR', '').str.replace(',', '').str.strip(), errors='coerce')
    df_test4_clean['price_sar'] = clean_price(df_test4_clean['price_sar'])
    df_prod_clean['price_sar'] = clean_price(df_prod_clean['price_sar'])

    def clean_units(unit_column):
        return unit_column.str.lower().str.strip()
    df_test4_clean['unit_of_measurement'] = clean_units(df_test4_clean['unit_of_measurement'])
    df_prod_clean['unit_of_measurement'] = clean_units(df_prod_clean['unit_of_measurement'])
    
    competitor_data = pd.concat([df_test4_clean, df_prod_clean], ignore_index=True)
    print(f"✅ Datos crudos combinados. Total: {len(competitor_data)} filas.")
    return competitor_data

def process_volumetric_data(df):
    print("\n--- 2a. Procesando productos volumétricos ---")
    df['unit_of_measurement'] = df['unit_of_measurement'].str.lower().str.strip()
    
    original_rows = len(df)
    df = df[df['unit_of_measurement'] != 'g']
    print(f"Se excluyeron {original_rows - len(df)} productos medidos en gramos.")

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
    
    df.to_csv('competitor_volumetric_processed.csv', index=False)
    print("✅ Archivo 'competitor_volumetric_processed.csv' guardado.")

def process_unit_data(df):
    print("\n--- 2b. Procesando productos por unidad ---")
    df['total_quantity'] = pd.to_numeric(df['total_quantity'], errors='coerce')
    df.dropna(subset=['total_quantity'], inplace=True)
    df = df[df['total_quantity'] > 0]
    
    df['price_per_item'] = df['price_sar'] / df['total_quantity']
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(subset=['price_per_item'], inplace=True)
    
    df.to_csv('competitor_unit_processed.csv', index=False)
    print("✅ Archivo 'competitor_unit_processed.csv' guardado.")

def main():
    try:
        competitor_data = load_and_clean_raw_data()
        
        volumetric_units = ['ml', 'l', 'fl oz', 'g']
        is_volumetric = competitor_data['unit_of_measurement'].str.lower().isin(volumetric_units)
        
        process_volumetric_data(competitor_data[is_volumetric].copy())
        process_unit_data(competitor_data[~is_volumetric].copy())
        
        print("\n🎉 Preprocesamiento de datos de la competencia completado.")
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: No se encontró el archivo. {e}")
    except Exception as e:
        print(f"\n❌ Ocurrió un error inesperado: {e}")

if __name__ == "__main__":
    main()