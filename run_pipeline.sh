# #!/bin/bash

# PROJECT_DIR="/home/ubuntu/wayakit-price-prediction" 
# CONDA_ENV_NAME="wayakit_env" 
# LOG_DIR="$PROJECT_DIR/logs"
# LAST_FULL_RUN_FILE="$LOG_DIR/last_full_run.txt"
# SCRIPT_LOG_FILE="$LOG_DIR/pipeline_execution.log"
# CRON_LOG_FILE="$LOG_DIR/cron_output.log"
# CONDA_BASE_PATH=$(conda info --base 2>/dev/null || echo "/home/ubuntu/miniconda3") 

# ODOO_PRODUCTS_FILE="$PROJECT_DIR/ml_model/wayakit_products.csv"
# NEW_PRODUCTS_TEMP_FILE="$PROJECT_DIR/ml_model/wayakit_new_products_temp.csv"
# ANALYSIS_FILE_FULL="$PROJECT_DIR/scraper/analysis-odoo.csv"
# ANALYSIS_FILE_PARTIAL="$PROJECT_DIR/scraper/analysis-odoo_partial.csv"
# COMPETITORS_FILE="$PROJECT_DIR/scraper/competitors_complete.csv"
# ODOO_QUOTES_FILE="$PROJECT_DIR/ml_model/wayakit_cotizations.csv"


# log_message() {
#     local message="$1"
#     local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
#     mkdir -p "$LOG_DIR"
#     echo "$timestamp - $message" >> "$SCRIPT_LOG_FILE"
# }

# run_command() {
#     local cmd="$1"
#     local description="$2"
#     log_message "Iniciando: $description"

#     if eval "$cmd" >> "$SCRIPT_LOG_FILE" 2>&1; then
#         log_message "Éxito: $description completado."
#         return 0
#     else
#         log_message "ERROR: $description falló. Revisa '$SCRIPT_LOG_FILE' para detalles."
#         return 1
#     fi
# }

# IS_FULL_RUN="false"
# NOW_TS=$(date +%s)
# THREE_MONTHS_AGO_TS=$(date -u -d '3 months ago' +%s)

# log_message "========================================"
# log_message "Iniciando Pipeline Wayakit"
# log_message "========================================"
# log_message "Verificando tipo de ejecución..."

# if [ ! -f "$LAST_FULL_RUN_FILE" ]; then
#     log_message "Archivo '$LAST_FULL_RUN_FILE' no encontrado. Ejecución COMPLETA requerida."
#     IS_FULL_RUN="true"
# else
#     LAST_RUN_STR=$(cat "$LAST_FULL_RUN_FILE")
#     LAST_RUN_TS=$(date -d "$LAST_RUN_STR" +%s 2>/dev/null)
#     if [ -z "$LAST_RUN_TS" ]; then
#          log_message "ERROR: Formato de fecha inválido en '$LAST_FULL_RUN_FILE': '$LAST_RUN_STR'. Ejecución COMPLETA requerida."
#          IS_FULL_RUN="true"
#     elif [ "$LAST_RUN_TS" -lt "$THREE_MONTHS_AGO_TS" ]; then
#          log_message "Última ejecución completa ($LAST_RUN_STR) fue hace más de 3 meses. Ejecución COMPLETA requerida."
#          IS_FULL_RUN="true"
#     else
#          log_message "Última ejecución completa ($LAST_RUN_STR) fue hace menos de 3 meses. Ejecución PARCIAL."
#          IS_FULL_RUN="false"
#     fi
# fi

# if [ "$IS_FULL_RUN" == "true" ]; then
#     echo "$(date -u '+%Y-%m-%d %H:%M:%S')" > "$LAST_FULL_RUN_FILE"
#     log_message "Timestamp de ejecución COMPLETA actualizado en '$LAST_FULL_RUN_FILE'."
# fi

# log_message "Cambiando a directorio: $PROJECT_DIR"
# cd "$PROJECT_DIR" || { log_message "ERROR: No se pudo acceder a $PROJECT_DIR"; exit 1; }

# log_message "Inicializando Conda..."
# source "${CONDA_BASE_PATH}/etc/profile.d/conda.sh" || { log_message "ERROR: No se pudo inicializar Conda en ${CONDA_BASE_PATH}"; exit 1; }

# log_message "Activando entorno Conda: $CONDA_ENV_NAME"
# conda activate "$CONDA_ENV_NAME" || { log_message "ERROR: No se pudo activar el entorno $CONDA_ENV_NAME"; exit 1; }
# log_message "Entorno Conda activado."



# log_message "--- PASO 1: Preparando lista de scraping ---"
# if [ "$IS_FULL_RUN" == "true" ]; then
#     run_command "python ml_model/odoo_api_products.py" "1a. Obtener TODOS los productos de Odoo (con fecha)" || exit 1
#     run_command "python scraper/odoo_api_connection_products.py --input_odoo_products_file '$ODOO_PRODUCTS_FILE' --output_analysis_file '$ANALYSIS_FILE_FULL'" "1b. Generar lista COMPLETA de scraping ($ANALYSIS_FILE_FULL)" || exit 1
#     ANALYSIS_FILE_TO_USE="$ANALYSIS_FILE_FULL"
#     SCRAPER_OUTPUT_MODE="overwrite"
# else
#     run_command "python ml_model/odoo_api_products.py" "1a. Obtener TODOS los productos de Odoo (con fecha)" || exit 1
#     run_command "python ml_model/filter_new_products.py --input '$ODOO_PRODUCTS_FILE' --timestamp '$LAST_FULL_RUN_FILE' --output '$NEW_PRODUCTS_TEMP_FILE'" "1b. Filtrar productos NUEVOS ($NEW_PRODUCTS_TEMP_FILE)" || exit 1
#     run_command "python scraper/odoo_api_connection_products.py --input_odoo_products_file '$NEW_PRODUCTS_TEMP_FILE' --output_analysis_file '$ANALYSIS_FILE_PARTIAL'" "1c. Generar lista PARCIAL de scraping ($ANALYSIS_FILE_PARTIAL)" || exit 1
#     ANALYSIS_FILE_TO_USE="$ANALYSIS_FILE_PARTIAL"
#     SCRAPER_OUTPUT_MODE="append"
# fi

# log_message "--- PASO 2: Ejecutando Scraping ---"

# if [ "$IS_FULL_RUN" == "true" ] && [ -f "$COMPETITORS_FILE" ]; then
#     log_message "Limpiando archivo de competidores existente para ejecución completa."
#     rm "$COMPETITORS_FILE"
# fi
# run_command "python scraper/main.py --analysis_file '$ANALYSIS_FILE_TO_USE' --output_mode '$SCRAPER_OUTPUT_MODE' --output_file '$COMPETITORS_FILE'" "2. Ejecutar scraping (Modo: $SCRAPER_OUTPUT_MODE)" || exit 1


# log_message "--- PASO 3: Obteniendo Cotizaciones Odoo ---"
# run_command "python ml_model/odoo_api_cotizations.py" "3. Obtener cotizaciones de Odoo" || exit 1

# run_command "python ml_model/odoo_api_competitor_products.py" "3.1 Obtain competitor products from Odoo" || exit 1

# log_message "--- PASO 4: Asegurando Productos Odoo ---"
# if [ ! -f "$ODOO_PRODUCTS_FILE" ]; then
#     log_message "Archivo $ODOO_PRODUCTS_FILE no encontrado, ejecutando de nuevo."
#     run_command "python ml_model/odoo_api_products.py" "4. Obtener productos de Odoo" || exit 1
# else
#     log_message "Archivo $ODOO_PRODUCTS_FILE ya existe desde paso anterior."
# fi

# log_message "--- PASO 5: Preprocesando Datos de Competencia ---"
# run_command "python ml_model/1a_preprocess_data.py" "5. Preprocesar datos de competencia" || exit 1

# log_message "--- PASO 6: Preparando Datos Wayakit para Predicción ---"
# run_command "python ml_model/1b_preprocess_data.py" "6. Preparar datos de Wayakit para predicción" || exit 1

# log_message "--- PASO 7: Entrenando Modelos ML ---"
# run_command "python ml_model/2_train_models.py" "7. Entrenar modelos ML" || exit 1

# log_message "--- PASO 8: Generando Predicciones de Precios ---"
# run_command "python ml_model/3_predicted_prices.py" "8. Generar predicciones de precios" || exit 1

# log_message "--- PASO 9: Subiendo Sugerencias a Odoo ---"
# run_command "python ml_model/odoo_api_price_suggestion.py" "9. Subir sugerencias de precios a Odoo" || exit 1

# log_message "Desactivando entorno Conda."
# conda deactivate

# log_message "========================================"
# log_message "Pipeline Wayakit completado."
# log_message "========================================"

# exit 0

#!/bin/bash

PROJECT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd ) 
CONDA_ENV_NAME="wayakit_env" 
LOG_DIR="$PROJECT_DIR/logs"
LAST_FULL_RUN_FILE="$LOG_DIR/last_full_run.txt"
SCRIPT_LOG_FILE="$PROJECT_DIR/logs/pipeline_execution.log" 
CRON_LOG_FILE="$PROJECT_DIR/logs/cron_output.log" 
CONDA_BASE_PATH=$(conda info --base 2>/dev/null || echo "/home/$USER/miniconda3")

ODOO_PRODUCTS_FILE="$PROJECT_DIR/ml_model/wayakit_products.csv"
NEW_PRODUCTS_TEMP_FILE="$PROJECT_DIR/ml_model/wayakit_new_products_temp.csv"
ANALYSIS_FILE_FULL="$PROJECT_DIR/scraper/analysis-odoo.csv"
ANALYSIS_FILE_PARTIAL="$PROJECT_DIR/scraper/analysis-odoo_partial.csv"
COMPETITORS_FILE="$PROJECT_DIR/scraper/competitors_complete.csv"
ODOO_QUOTES_FILE="$PROJECT_DIR/ml_model/wayakit_cotizations.csv"

log_message() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    mkdir -p "$LOG_DIR"
    echo "$timestamp - $message" >> "$SCRIPT_LOG_FILE"
}

run_command() {
    local cmd="$1"
    local description="$2"
    log_message "Iniciando: $description"

    if eval "$cmd" >> "$SCRIPT_LOG_FILE" 2>&1; then
        log_message "Éxito: $description completado."
        return 0
    else
        log_message "ERROR: $description falló. Revisa '$SCRIPT_LOG_FILE' para detalles."
        return 1
    fi
}

# --- INICIO DE CAMBIOS: Lógica de Etapas (Stages) ---
STAGE=1 # Por defecto, ejecutamos la Etapa 1
if [ "$1" == "--stage" ] && [ "$2" == "2" ]; then
    STAGE=2
    log_message "Iniciando Pipeline en ETAPA 2 (Ejecución post-revisión manual)."
else
    log_message "Iniciando Pipeline en ETAPA 1 (Revisión de fecha y preparación)."
fi
# --- FIN DE CAMBIOS ---

# --- INICIO DE CAMBIOS: Condicional de Etapa 1 ---
# Toda la lógica de decisión y la ejecución completa/parcial-preparación 
# solo ocurre si estamos en la Etapa 1.
if [ "$STAGE" == "1" ]; then

    IS_FULL_RUN="false"
    NOW_TS=$(date +%s)
    THREE_MONTHS_AGO_TS=$(date -u -d '3 months ago' +%s)

    log_message "========================================"
    log_message "Iniciando Pipeline Wayakit (ETAPA 1)"
    log_message "========================================"
    log_message "Verificando tipo de ejecución..."

    if [ ! -f "$LAST_FULL_RUN_FILE" ]; then
        log_message "Archivo '$LAST_FULL_RUN_FILE' no encontrado. Ejecución COMPLETA requerida."
        IS_FULL_RUN="true"
    else
        LAST_RUN_STR=$(cat "$LAST_FULL_RUN_FILE")
        LAST_RUN_TS=$(date -d "$LAST_RUN_STR" +%s 2>/dev/null)
        if [ -z "$LAST_RUN_TS" ]; then
             log_message "ERROR: Formato de fecha inválido en '$LAST_FULL_RUN_FILE': '$LAST_RUN_STR'. Ejecución COMPLETA requerida."
             IS_FULL_RUN="true"
        elif [ "$LAST_RUN_TS" -lt "$THREE_MONTHS_AGO_TS" ]; then
             log_message "Última ejecución completa ($LAST_RUN_STR) fue hace más de 3 meses. Ejecución COMPLETA requerida."
             IS_FULL_RUN="true"
        else
             log_message "Última ejecución completa ($LAST_RUN_STR) fue hace menos de 3 meses. Ejecución PARCIAL."
             IS_FULL_RUN="false"
        fi
    fi

    if [ "$IS_FULL_RUN" == "true" ]; then
        echo "$(date -u '+%Y-%m-%d %H:%M:%S')" > "$LAST_FULL_RUN_FILE"
        log_message "Timestamp de ejecución COMPLETA actualizado en '$LAST_FULL_RUN_FILE'."
    fi

    log_message "Cambiando a directorio: $PROJECT_DIR"
    cd "$PROJECT_DIR" || { log_message "ERROR: No se pudo acceder a $PROJECT_DIR"; exit 1; }

    log_message "Inicializando Conda..."
    source "${CONDA_BASE_PATH}/etc/profile.d/conda.sh" || { log_message "ERROR: No se pudo inicializar Conda en ${CONDA_BASE_PATH}"; exit 1; }

    log_message "Activando entorno Conda: $CONDA_ENV_NAME"
    conda activate "$CONDA_ENV_NAME" || { log_message "ERROR: No se pudo activar el entorno $CONDA_ENV_NAME"; exit 1; }
    log_message "Entorno Conda activado."


    log_message "--- PASO 1: Preparando lista de scraping ---"
    if [ "$IS_FULL_RUN" == "true" ]; then
        # --- EJECUCIÓN COMPLETA (Trimestral) ---
        # Ejecuta todo el pipeline de principio a fin.
        log_message "Modo: EJECUCIÓN COMPLETA (Trimestral)"
        run_command "python ml_model/odoo_api_products.py" "1a. Obtener TODOS los productos de Odoo (con fecha)" || exit 1
        run_command "python scraper/odoo_api_connection_products.py --input_odoo_products_file '$ODOO_PRODUCTS_FILE' --output_analysis_file '$ANALYSIS_FILE_FULL'" "1b. Generar lista COMPLETA de scraping ($ANALYSIS_FILE_FULL)" || exit 1
        ANALYSIS_FILE_TO_USE="$ANALYSIS_FILE_FULL"
        SCRAPER_OUTPUT_MODE="overwrite"

        log_message "--- PASO 2: Ejecutando Scraping ---"
        if [ -f "$COMPETITORS_FILE" ]; then
            log_message "Limpiando archivo de competidores existente para ejecución completa."
            rm "$COMPETITORS_FILE"
        fi
        run_command "python scraper/main.py --analysis_file '$ANALYSIS_FILE_TO_USE' --output_mode '$SCRAPER_OUTPUT_MODE' --output_file '$COMPETITORS_FILE'" "2. Ejecutar scraping (Modo: $SCRAPER_OUTPUT_MODE)" || exit 1

        # El resto de los pasos...
        log_message "--- PASO 3: Obteniendo Cotizaciones Odoo ---"
        run_command "python ml_model/odoo_api_cotizations.py" "3. Obtener cotizaciones de Odoo" || exit 1
        run_command "python ml_model/odoo_api_competitor_products.py" "3.1 Obtain competitor products from Odoo" || exit 1
        
        log_message "--- PASO 4: (Omitido, ya se ejecutó 1a) ---"

        log_message "--- PASO 5: Preprocesando Datos de Competencia ---"
        run_command "python ml_model/1a_preprocess_data.py" "5. Preprocesar datos de competencia" || exit 1

        log_message "--- PASO 6: Preparando Datos Wayakit para Predicción ---"
        run_command "python ml_model/1b_preprocess_data.py" "6. Preparar datos de Wayakit para predicción" || exit 1

        log_message "--- PASO 7: Entrenando Modelos ML ---"
        run_command "python ml_model/2_train_models.py" "7. Entrenar modelos ML" || exit 1

        log_message "--- PASO 8: Generando Predicciones de Precios ---"
        run_command "python ml_model/3_predicted_prices.py" "8. Generar predicciones de precios" || exit 1

        log_message "--- PASO 9: Subiendo Sugerencias a Odoo ---"
        run_command "python ml_model/odoo_api_price_suggestion.py --run_mode full" "9. Subir sugerencias de precios a Odoo" || exit 1

        log_message "Pipeline COMPLETO finalizado."

    else
        # --- EJECUCIÓN PARCIAL - ETAPA 1 (Semanal - Jueves) ---
        # Ejecuta solo la preparación y luego se detiene.
        log_message "Modo: EJECUCIÓN PARCIAL (ETAPA 1 - Preparación)"
        run_command "python ml_model/odoo_api_products.py" "1a. Obtener TODOS los productos de Odoo (con fecha)" || exit 1
        run_command "python ml_model/filter_new_products.py --input '$ODOO_PRODUCTS_FILE' --timestamp '$LAST_FULL_RUN_FILE' --output '$NEW_PRODUCTS_TEMP_FILE'" "1b. Filtrar productos NUEVOS ($NEW_PRODUCTS_TEMP_FILE)" || exit 1
        
        log_message "ETAPA 1 (Preparación) completada. El script se detendrá."
        log_message "El archivo '$NEW_PRODUCTS_TEMP_FILE' está listo para revisión manual."
        log_message "Ejecute 'run_pipeline.sh --stage 2' para continuar."
    fi

    log_message "Desactivando entorno Conda."
    conda deactivate
    log_message "========================================"
    exit 0

fi
# --- FIN DE CAMBIOS: Condicional de Etapa 1 ---


# --- INICIO DE CAMBIOS: Condicional de Etapa 2 ---
# Esta sección solo se ejecuta si el script es llamado con --stage 2
if [ "$STAGE" == "2" ]; then

    # --- INICIO DE NUEVA VERIFICACIÓN ---
    log_message "========================================"
    log_message "Iniciando Pipeline Wayakit (ETAPA 2)"
    log_message "========================================"
    log_message "Verificando si hay productos nuevos en '$NEW_PRODUCTS_TEMP_FILE'..."

    # Contar líneas en el archivo temporal (wc -l)
    # 'xargs' se usa para quitar espacios en blanco del resultado de wc
    LINE_COUNT=$(wc -l < "$NEW_PRODUCTS_TEMP_FILE" | xargs) 

    # Si hay 1 línea (solo cabecera) o 0 líneas (vacío)
    if [ "$LINE_COUNT" -le 1 ]; then
        log_message "No se encontraron productos nuevos ($LINE_COUNT líneas). No se requiere ejecución."
        log_message "Omitiendo ETAPA 2."
        log_message "========================================"
        exit 0 # Salir exitosamente (código 0)
    fi
    
    log_message "Se encontraron $LINE_COUNT productos nuevos (incluyendo cabecera). Continuando con la ejecución..."
    # --- FIN DE NUEVA VERIFICACIÓN ---

    log_message "Cambiando a directorio: $PROJECT_DIR"
    cd "$PROJECT_DIR" || { log_message "ERROR: No se pudo acceder a $PROJECT_DIR"; exit 1; }

    log_message "Inicializando Conda..."
    source "${CONDA_BASE_PATH}/etc/profile.d/conda.sh" || { log_message "ERROR: No se pudo inicializar Conda en ${CONDA_BASE_PATH}"; exit 1; }

    log_message "Activando entorno Conda: $CONDA_ENV_NAME"
    conda activate "$CONDA_ENV_NAME" || { log_message "ERROR: No se pudo activar el entorno $CONDA_ENV_NAME"; exit 1; }
    log_message "Entorno Conda activado."

    log_message "--- PASO 1c: Continuando Ejecución Parcial ---"
    # Asume que los pasos 1a y 1b ya se ejecutaron en la Etapa 1
    # Asume que la revisión manual de 'modifiers_mapping.csv' ya se hizo
    run_command "python scraper/odoo_api_connection_products.py --input_odoo_products_file '$NEW_PRODUCTS_TEMP_FILE' --output_analysis_file '$ANALYSIS_FILE_PARTIAL'" "1c. Generar lista PARCIAL de scraping (post-revisión)" || exit 1
    ANALYSIS_FILE_TO_USE="$ANALYSIS_FILE_PARTIAL"
    SCRAPER_OUTPUT_MODE="append"

    log_message "--- PASO 2: Ejecutando Scraping ---"
    run_command "python scraper/main.py --analysis_file '$ANALYSIS_FILE_TO_USE' --output_mode '$SCRAPER_OUTPUT_MODE' --output_file '$COMPETITORS_FILE'" "2. Ejecutar scraping (Modo: $SCRAPER_OUTPUT_MODE)" || exit 1

    log_message "--- PASO 3: Obteniendo Cotizaciones Odoo ---"
    run_command "python ml_model/odoo_api_cotizations.py" "3. Obtener cotizaciones de Odoo" || exit 1
    run_command "python ml_model/odoo_api_competitor_products.py" "3.1 Obtain competitor products from Odoo" || exit 1

    log_message "--- PASO 4: Asegurando Productos Odoo ---"
    if [ ! -f "$ODOO_PRODUCTS_FILE" ]; then
        log_message "Archivo $ODOO_PRODUCTS_FILE no encontrado, ejecutando de nuevo."
        run_command "python ml_model/odoo_api_products.py" "4. Obtener productos de Odoo" || exit 1
    else
        log_message "Archivo $ODOO_PRODUCTS_FILE ya existe desde Etapa 1."
    fi

    log_message "--- PASO 5: Preprocesando Datos de Competencia ---"
    run_command "python ml_model/1a_preprocess_data.py" "5. Preprocesar datos de competencia" || exit 1

    log_message "--- PASO 6: Preparando Datos Wayakit para Predicción ---"
    run_command "python ml_model/1b_preprocess_data.py" "6. Preparar datos de Wayakit para predicción" || exit 1

    log_message "--- PASO 7: Entrenando Modelos ML ---"
    run_command "python ml_model/2_train_models.py" "7. Entrenar modelos ML" || exit 1

    log_message "--- PASO 8: Generando Predicciones de Precios ---"
    run_command "python ml_model/3_predicted_prices.py" "8. Generar predicciones de precios" || exit 1

    log_message "--- PASO 9: Subiendo Sugerencias a Odoo ---"
    run_command "python ml_model/odoo_api_price_suggestion.py --run_mode partial" "9. Subir sugerencias de precios a Odoo" || exit 1

    log_message "Desactivando entorno Conda."
    conda deactivate

    log_message "========================================"
    log_message "Pipeline Wayakit (ETAPA 2) completado."
    log_message "========================================"
    exit 0
fi
# --- FIN DE CAMBIOS: Condicional de Etapa 2 ---