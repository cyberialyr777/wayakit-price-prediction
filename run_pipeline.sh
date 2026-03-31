#!/bin/bash
set -o pipefail

# --- CONFIGURACIÓN ---
PROJECT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONDA_ENV_NAME="wayakit_env"
LOG_DIR="$PROJECT_DIR/logs"

# --- Archivos de Estado y Logs ---
LAST_FULL_RUN_FILE="$LOG_DIR/last_full_run.txt"
RUN_MODE_FILE="$LOG_DIR/run_mode.txt" # Archivo de estado
SCRIPT_LOG_FILE="$PROJECT_DIR/logs/pipeline_execution.log"
# --- CAMBIO IMPORTANTE: Detectar usuario (ubuntu o ec2-user) ---
if [ -d "/home/ubuntu" ]; then
    CONDA_BASE_PATH=$(conda info --base 2>/dev/null || echo "/home/ubuntu/miniconda3")
else
    CONDA_BASE_PATH=$(conda info --base 2>/dev/null || echo "/home/ec2-user/miniconda3")
fi

# --- Archivos de Datos ---
ODOO_PRODUCTS_FILE="$PROJECT_DIR/ml_model/wayakit_products.csv"
NEW_PRODUCTS_TEMP_FILE="$PROJECT_DIR/ml_model/wayakit_new_products_temp.csv"
ANALYSIS_FILE_FULL="$PROJECT_DIR/scraper/analysis-odoo.csv"
ANALYSIS_FILE_PARTIAL="$PROJECT_DIR/scraper/analysis-odoo_partial.csv"
COMPETITORS_FILE="$PROJECT_DIR/scraper/competitors_complete.csv"

# --- Funciones de Logging ---
log_message() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    mkdir -p "$LOG_DIR"
    echo "$timestamp - $message" | tee -a "$SCRIPT_LOG_FILE"
}

run_command() {
    local cmd="$1"
    local description="$2"
    log_message "Iniciando: $description"
    eval "$cmd" 2>&1 | tee -a "$SCRIPT_LOG_FILE"
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_message "Éxito: $description completado."
        return 0
    else
        log_message "ERROR: $description falló. Revisa '$SCRIPT_LOG_FILE' para detalles."
        return 1
    fi
}

# --- Lógica de Etapas (Stages) ---
STAGE=1
if [ "$1" == "--stage" ] && [ "$2" == "2" ]; then
    STAGE=2
else
    STAGE=1
fi

# ==================================================================
# --- INICIO DE ETAPA 1 (Preparación y Apagado) ---
# ==================================================================
if [ "$STAGE" == "1" ]; then

    IS_FULL_RUN="false"
    NOW_TS=$(date -u +%s)
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
        LAST_RUN_TS=$(date -u -d "$LAST_RUN_STR" +%s 2>/dev/null)
        if [ -z "$LAST_RUN_TS" ]; then
             log_message "ERROR: Formato de fecha inválido. Ejecución COMPLETA requerida."
             IS_FULL_RUN="true"
        elif [ "$LAST_RUN_TS" -lt "$THREE_MONTHS_AGO_TS" ]; then
             log_message "Última ejecución completa ($LAST_RUN_STR) fue hace más de 3 meses. Ejecución COMPLETA requerida."
             IS_FULL_RUN="true"
        else
             log_message "Última ejecución completa ($LAST_RUN_STR) es reciente. Ejecución PARCIAL."
             IS_FULL_RUN="false"
        fi
    fi

    log_message "Cambiando a directorio: $PROJECT_DIR"
    cd "$PROJECT_DIR" || { log_message "ERROR: No se pudo acceder a $PROJECT_DIR"; exit 1; }

    log_message "Inicializando Conda..."
    source "${CONDA_BASE_PATH}/etc/profile.d/conda.sh" || { log_message "ERROR: No se pudo inicializar Conda"; exit 1; }

    log_message "Activando entorno Conda: $CONDA_ENV_NAME"
    conda activate "$CONDA_ENV_NAME" || { log_message "ERROR: No se pudo activar el entorno $CONDA_ENV_NAME"; exit 1; }
    log_message "Entorno Conda activado."

    log_message "--- PASO 1: Preparando lista de scraping ---"
    run_command "python ml_model/odoo_api_products.py" "1a. Obtener TODOS los productos de Odoo (con fecha)" || exit 1

    if [ "$IS_FULL_RUN" == "true" ]; then
        log_message "Modo: EJECUCIÓN COMPLETA (Preparación)"
        run_command "python scraper/odoo_api_connection_products.py --input_odoo_products_file '$ODOO_PRODUCTS_FILE' --output_analysis_file '$ANALYSIS_FILE_FULL'" "1b. Generar lista COMPLETA de scraping" || exit 1
        echo "full" > "$RUN_MODE_FILE"
        echo "$(date -u '+%Y-%m-%d %H:%M:%S')" > "$LAST_FULL_RUN_FILE"
        log_message "Archivo '$ANALYSIS_FILE_FULL' está listo para revisión manual."

    else
        log_message "Modo: EJECUCIÓN PARCIAL (Preparación)"
        run_command "python ml_model/filter_new_products.py --input '$ODOO_PRODUCTS_FILE' --timestamp '$LAST_FULL_RUN_FILE' --output '$NEW_PRODUCTS_TEMP_FILE'" "1b. Filtrar productos NUEVOS" || exit 1
        echo "partial" > "$RUN_MODE_FILE"
        log_message "Archivo '$NEW_PRODUCTS_TEMP_FILE' está listo."
    fi

    log_message "--- ETAPA 1 (Preparación) completada ---"
    log_message "Desactivando entorno Conda."
    conda deactivate
    
    # --- Apagado automático ---
    log_message "El servidor se apagará en 1 minuto."
    log_message "========================================"
    sudo shutdown -h +1
    exit 0

fi
# --- FIN DE ETAPA 1 ---


# ==================================================================
# --- INICIO DE ETAPA 2 (Ejecución Post-Revisión) ---
# ==================================================================
if [ "$STAGE" == "2" ]; then

    log_message "========================================"
    log_message "Iniciando Pipeline Wayakit (ETAPA 2)"
    log_message "========================================"

    if [ ! -f "$RUN_MODE_FILE" ]; then
        log_message "ERROR: Archivo de estado '$RUN_MODE_FILE' no encontrado."
        exit 1
    fi

    RUN_MODE=$(cat "$RUN_MODE_FILE")
    log_message "Modo de continuación detectado: $RUN_MODE"
    
    # Asegúrate de que los cambios manuales de Git se apliquen
    log_message "Actualizando proyecto desde Git..."
    cd "$PROJECT_DIR" || { log_message "ERROR: No se pudo acceder a $PROJECT_DIR"; exit 1; }
    git pull
    
    log_message "Inicializando Conda..."
    source "${CONDA_BASE_PATH}/etc/profile.d/conda.sh" || { log_message "ERROR: No se pudo inicializar Conda"; exit 1; }

    log_message "Activando entorno Conda: $CONDA_ENV_NAME"
    conda activate "$CONDA_ENV_NAME" || { log_message "ERROR: No se pudo activar el entorno $CONDA_ENV_NAME"; exit 1; }
    log_message "Entorno Conda activado."
    
    
    # --- INICIO: Flujo de EJECUCIÓN COMPLETA (Etapa 2) ---
    if [ "$RUN_MODE" == "full" ]; then
        log_message "Continuando con flujo COMPLETO..."
        ANALYSIS_FILE_TO_USE="$ANALYSIS_FILE_FULL"
        SCRAPER_OUTPUT_MODE="overwrite"
        
        if [ -f "$COMPETITORS_FILE" ]; then
            log_message "Limpiando archivo de competidores existente."
            rm "$COMPETITORS_FILE"
        fi
        
        run_command "timeout 30h python scraper/main.py --analysis_file '$ANALYSIS_FILE_TO_USE' --output_mode '$SCRAPER_OUTPUT_MODE' --output_file '$COMPETITORS_FILE'" "2. Ejecutar scraping (Modo: $SCRAPER_OUTPUT_MODE)"
        run_command "python ml_model/odoo_api_cotizations.py" "3a. Obtener cotizaciones de Odoo"
        run_command "python ml_model/odoo_api_competitor_products.py" "3b. Obtener productos de competidores de Odoo"
        
        if [ ! -f "$ODOO_PRODUCTS_FILE" ]; then
            run_command "python ml_model/odoo_api_products.py" "4. (Re)Obtener productos de Odoo"
        fi

        # --- CAMBIO: Añadido 'timeout 1h' (1 hora) a los scripts de ML ---
        run_command "timeout 1h python ml_model/1a_preprocess_data.py" "5. Preprocesar datos de competencia"
        run_command "timeout 1h python ml_model/1b_preprocess_data.py --run_mode full" "6. Preparar datos Wayakit (Modo: full)"
        run_command "timeout 1h python ml_model/2_train_models.py" "7. Entrenar modelos ML"
        run_command "timeout 1h python ml_model/3_predicted_prices.py" "8. Generar predicciones"
        run_command "timeout 1h python ml_model/odoo_api_price_suggestion.py --run_mode full" "9. Subir sugerencias a Odoo (Modo: full)"

    # --- FIN: Flujo de EJECUCIÓN COMPLETA (Etapa 2) ---

    # --- INICIO: Flujo de EJECUCIÓN PARCIAL (Etapa 2) ---
    elif [ "$RUN_MODE" == "partial" ]; then

        log_message "Continuando con flujo PARCIAL..."
        log_message "Verificando si hay productos nuevos en '$NEW_PRODUCTS_TEMP_FILE'..."
        LINE_COUNT=$(wc -l < "$NEW_PRODUCTS_TEMP_FILE" | xargs) 

        if [ "$LINE_COUNT" -le 1 ]; then
            log_message "No se encontraron productos nuevos ($LINE_COUNT líneas). No se requiere ejecución."
            log_message "Omitiendo ETAPA 2."
        else
            log_message "Se encontraron $LINE_COUNT productos nuevos. Continuando..."
            
            ANALYSIS_FILE_TO_USE="$ANALYSIS_FILE_PARTIAL"
            SCRAPER_OUTPUT_MODE="append"
            # --- CAMBIO: Añadido 'timeout 4h' (4 horas) al scraping parcial ---
            run_command "python scraper/odoo_api_connection_products.py --input_odoo_products_file '$NEW_PRODUCTS_TEMP_FILE' --output_analysis_file '$ANALYSIS_FILE_PARTIAL'" "1c. Generar lista PARCIAL de scraping (post-revisión)"
            run_command "timeout 4h python scraper/main.py --analysis_file '$ANALYSIS_FILE_TO_USE' --output_mode '$SCRAPER_OUTPUT_MODE' --output_file '$COMPETITORS_FILE'" "2. Ejecutar scraping (Modo: $SCRAPER_OUTPUT_MODE)"
            run_command "python ml_model/odoo_api_cotizations.py" "3a. Obtener cotizaciones de Odoo"
            run_command "python ml_model/odoo_api_competitor_products.py" "3b. Obtener productos de competidores de Odoo"

            if [ ! -f "$ODOO_PRODUCTS_FILE" ]; then
                run_command "python ml_model/odoo_api_products.py" "4. (Re)Obtener productos de Odoo"
            fi
            
            # --- CAMBIO: Añadido 'timeout 1h' (1 hora) a los scripts de ML ---
            run_command "timeout 1h python ml_model/1a_preprocess_data.py" "5. Preprocesar datos de competencia"
            run_command "timeout 1h python ml_model/1b_preprocess_data.py --run_mode partial" "6. Preparar datos Wayakit (Modo: partial)"
            run_command "timeout 1h python ml_model/2_train_models.py" "7. Entrenar modelos ML"
            run_command "timeout 1h python ml_model/3_predicted_prices.py" "8. Generar predicciones"
            run_command "timeout 1h python ml_model/odoo_api_price_suggestion.py --run_mode partial" "9. Subir sugerencias a Odoo (Modo: partial)"
        fi
    
    else
        log_message "ERROR: Modo de ejecución '$RUN_MODE' desconocido."
    fi
    # --- FIN: Flujo de EJECUCIÓN PARCIAL (Etapa 2) ---

    log_message "Desactivando entorno Conda."
    conda deactivate
    
    # --- Apagado INMEDIATO de Etapa 2 ---
    log_message "Pipeline Wayakit (ETAPA 2) completado."
    log_message "El servidor se apagará en 1 minuto."
    log_message "========================================"
    
    sudo shutdown -h +1
    exit 0
fi
# --- FIN DE ETAPA 2 ---