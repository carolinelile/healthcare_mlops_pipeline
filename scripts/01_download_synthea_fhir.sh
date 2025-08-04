
#!/bin/bash

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(dirname "$(dirname "$script_dir")")"

set -e 
set -o pipefail

if [[ ! -f "$project_root/.env" ]]; then
  echo ".env file not found at $project_root/.env"
  exit 1
fi

set -a  # export all variables in .env
source "$project_root/.env"
set +a

timestamp=$(date +%Y%m%d-%H%M)
output_dir="data/raw/$timestamp"
log_file="logs/01_download_synthea_$timestamp.log"
gcs_log_path="$LOG_DIR/$(basename "$log_file")"

# Create output and log directories
mkdir -p "$output_dir"
mkdir -p logs

# Redirect all output to log file and console
exec > >(tee -a "$log_file") 2>&1

echo "$(date) - Script started"
echo "1.1.1 Downloading Synthea software"
wget -nc $JAR_URL  # -nc = no clobber (skip download if file exists)

echo "1.1.2 Generating $PATIENT_COUNT synthetic patients for $STATE..."
mkdir -p $output_dir
java -jar $JAR_NAME -p $PATIENT_COUNT $STATE \
  --exporter.baseDirectory="$output_dir" \
  --module hypertension \
  --module hyperlipidemia \
  --module diabetes \
  --module coronary_artery_disease \
  --module heart_failure \
  --module stroke \
  --module obesity \
  --module smoking \
  --module alcoholabuse \
  --module sleep_apnea

echo "1.1.3 Synthea generation completed"
echo "Data saved to: '$output_dir'"

# Upload log to GCS
gsutil cp "$log_file" "gs://$GCS_BUCKET/$gcs_log_path"
echo "1.1.4 Log uploaded to GCS: gs://$GCS_BUCKET/$gcs_log_path"
