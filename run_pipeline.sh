#!/bin/bash

PROJECT_DIR="/home/ubuntu/wayakit-price-prediction"

CONDA_ENV_NAME="wayakit_env" 

SCRIPT_LOG_FILE="$PROJECT_DIR/logs/pipeline_execution.log"

CONDA_BASE_PATH=$(conda info --base)

log_message() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "$timestamp - $message" >> "$SCRIPT_LOG_FILE"
}

run_command() {
    local cmd="$1"
    local description="$2"

    log_message "Starting: $description"
    if conda run -n "$CONDA_ENV_NAME" $cmd >> "$SCRIPT_LOG_FILE" 2>&1; then
        log_message "Success: $description completed."
        return 0 
    else
        log_message "ERROR: $description failed. Check '$SCRIPT_LOG_FILE' and '$LOG_FILE' for details."
        return 1
    fi
}

log_message "========================================"
log_message "Starting Wayakit pipeline execution"
log_message "========================================"

cd "$PROJECT_DIR" || { log_message "ERROR: Could not access directory $PROJECT_DIR"; exit 1; }

log_message "Initializing Conda for script..."
source "${CONDA_BASE_PATH}/etc/profile.d/conda.sh"
if [ $? -ne 0 ]; then
    log_message "ERROR: Could not initialize Conda. Check CONDA_BASE_PATH: ${CONDA_BASE_PATH}"
    exit 1
log_message "Activating Conda environment: $CONDA_ENV_NAME"
conda activate "$CONDA_ENV_NAME"
if [ $? -ne 0 ]; then
    log_message "ERROR: Could not activate Conda environment '$CONDA_ENV_NAME'."
    exit 1
fi
log_message "Conda environment activated."

run_command "python scraper/odoo_api_connection_products.py" "Prepare data for scraping (analysis-odoo.csv)" || exit 1

run_command "python scraper/main.py" "Execute competitor scraping" || exit 1

run_command "python ml_model/odoo_api_cotizations.py" "Get quotations from Odoo" || exit 1

run_command "python ml_model/odoo_api_products.py" "Get products from Odoo" || exit 1

run_command "python ml_model/1a_preprocess_data.py" "Preprocess competitor data" || exit 1

run_command "python ml_model/1b_preprocess_data.py" "Prepare Wayakit data for prediction" || exit 1

run_command "python ml_model/2_train_models.py" "Train ML models" || exit 1

run_command "python ml_model/3_predicted_prices.py" "Generate price predictions" || exit 1

run_command "python ml_model/odoo_api_price_suggestion.py" "Upload price suggestions to Odoo" || exit 1

log_message "========================================"
log_message "Wayakit pipeline completed successfully."
log_message "========================================"

log_message "Deactivating Conda environment."
conda deactivate

exit 0