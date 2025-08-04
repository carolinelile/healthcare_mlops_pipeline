
import os
import json
import logging
from google.cloud import storage
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent
env_path = project_root / ".env"
if not env_path.exists():
    raise FileNotFoundError(f".env file not found at {env_path}")
load_dotenv(dotenv_path=env_path)

gcs_bucket = os.environ.get("GCS_BUCKET")
log_dir = project_root / os.environ.get("LOG_DIR")
local_data_root = project_root / "data/raw"

# Get the latest raw data folder locally
timestamp = sorted(os.listdir(local_data_root))[-1]
local_fhir_dir = os.path.join(local_data_root, timestamp, "fhir")
local_metadata_dir = os.path.join(local_data_root, timestamp, "metadata")

# GCS folder to upload to
gcs_fhir_prefix = f"raw_data/{timestamp}/fhir/"
gcs_metadata_prefix = f"raw_data/{timestamp}/metadata/"

# Logging
runtime_timestamp = datetime.now().strftime("%Y%m%d-%H%M")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, f"02_ingest_fhir_to_gcs_{runtime_timestamp}.log")
logging.basicConfig(
    filename=log_path,
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)

# Init GCS client
client = storage.Client()
bucket = client.bucket(gcs_bucket)

def is_valid_json(file_path):
    """Basic data quality check: check if file is valid JSON."""
    try:
        with open(file_path, "r") as f:
            json.load(f)
        return True
    except json.JSONDecodeError:
        return False

def upload_directory(local_dir, gcs_prefix):
    """Upload raw data to GCS bucket."""
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)

            if not is_valid_json(local_path):
                logging.warning(f"Skipping invalid JSON file: {local_path}")
                continue

            relative_path = os.path.relpath(local_path, local_dir)
            gcs_path = os.path.join(gcs_prefix, relative_path).replace("\\", "/")
            blob = bucket.blob(gcs_path)

            # Skip if file already exists in GCS, ensures resumable and idempotent uploads
            if blob.exists(client):
                logging.debug(f"Skipping already-uploaded file: {local_path}")
                continue

            blob.upload_from_filename(local_path)
            logging.info(f"Uploaded {local_path} → gs://{gcs_bucket}/{gcs_path}")

def main():
    print(f"1.2.1 Uploading FHIR files from: {local_fhir_dir}")
    upload_directory(local_fhir_dir, gcs_fhir_prefix)

    print(f"1.2.2 Uploading Metadata files from: {local_metadata_dir}")
    upload_directory(local_metadata_dir, gcs_metadata_prefix)

    # Upload the log to GCS
    gcs_log_path = f"logs/{os.path.basename(log_path)}"
    log_blob = bucket.blob(gcs_log_path)
    log_blob.upload_from_filename(log_path)
    
    print(f"1.2.3 Log uploaded to GCS: gs://{gcs_bucket}/{gcs_log_path}")
    # logging.info(f"Log uploaded to GCS: gs://{gcs_bucket}/{gcs_log_path}")

    print("Ingestion to GCS complete.")
    # logging.info("FHIR & metadata ingestion to GCS completed.")

if __name__ == "__main__":
    main()
