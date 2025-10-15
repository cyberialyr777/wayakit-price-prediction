    
import pandas as pd
import os

def generar_lista_prediccion():
    """
    Esta función crea el archivo 'wayakit_products_to_predict_odoo1.csv'
    filtrando un catálogo maestro y conservando todas las columnas originales.
    """
    # --- Archivos necesarios ---
    archivo_volumetrico = 'competitor_volumetric_processed.csv'
    archivo_unidades = 'competitor_unit_processed.csv'
    catalogo_maestro = 'wayakit_products.csv'
    archivo_salida = 'wayakit_products_to_predict_odoo.csv'

    # --- Verificación de archivos ---
    archivos_necesarios = [archivo_volumetrico, archivo_unidades, catalogo_maestro]
    for archivo in archivos_necesarios:
        if not os.path.exists(archivo):
            print(f"❌ ERROR: El archivo '{archivo}' no se encontró.")
            print("➡️ Solución: Asegúrate de haber ejecutado primero tu notebook principal.")
            return

    try:
        # --- Paso 1: Extraer las Categorías Conocidas por los Modelos ---
        print("\nPASO 1: Identificando productos que el modelo conoce...")
        df_vol_comp = pd.read_csv(archivo_volumetrico)
        df_unit_comp = pd.read_csv(archivo_unidades)
        known_product_types = set(list(df_vol_comp['type_of_product'].unique()) + list(df_unit_comp['type_of_product'].unique()))
        print(f"✅ Se identificaron {len(known_product_types)} tipos de productos que el modelo puede predecir.")

        # --- Paso 2: Cargar tu Nuevo Catálogo de Productos ---
        print("\nPASO 2: Cargando el catálogo de productos maestro...")
        df_catalogue = pd.read_csv(catalogo_maestro, encoding='utf-8-sig')
        print(f"✅ Se cargaron {len(df_catalogue)} productos del catálogo.")

        # --- Paso 3: Filtrar el Catálogo ---
        print("\nPASO 3: Filtrando el catálogo para encontrar productos compatibles...")
        df_catalogue['Type_of_product'] = df_catalogue['Type_of_product'].str.strip()
        df_predictable = df_catalogue[df_catalogue['Type_of_product'].isin(known_product_types)].copy()
        print(f"✅ Se encontraron {len(df_predictable)} productos compatibles.")
        
        # --- Paso 4: Guardar el Nuevo Archivo ---
        print(f"\nPASO 4: Guardando el nuevo archivo como '{archivo_salida}'...")
        # Se guarda el DataFrame filtrado 'df_predictable' con todas sus columnas
        df_predictable.to_csv(archivo_salida, index=False)
        
        print("\n🎉 ¡Proceso completado! 🎉")
        print(f"El archivo '{archivo_salida}' está listo con los productos filtrados y todas sus columnas originales.")

    except FileNotFoundError:
        print(f"❌ ERROR: Uno de los archivos base no existe. Re-ejecuta el notebook principal.")
    except KeyError as e:
        # Este error es menos probable ahora, pero se mantiene por seguridad
        print(f"❌ ERROR: No se encontró la columna 'Type_of_product': {e}")
        print("➡️ Solución: Revisa que esa columna exista en tus archivos CSV.")
    except Exception as e:
        print(f"\n❌ Ocurrió un error inesperado durante el proceso: {e}")

# --- Ejecutar la función ---
if __name__ == "__main__":
    generar_lista_prediccion()