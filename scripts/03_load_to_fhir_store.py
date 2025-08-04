from googleapiclient import discovery
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
import time

# Load .env
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent
env_path = project_root / ".env"
if not env_path.exists():
    raise FileNotFoundError(f".env file not found at {env_path}")
load_dotenv(dotenv_path=env_path)

project_id = os.environ.get("PROJECT_ID")
location = os.environ.get("LOCATION")
dataset_id = os.environ.get("DATASET_ID")
fhir_store_id = os.environ.get("FHIR_STORE_ID")
gcs_bucket = os.environ.get("GCS_BUCKET")
gcs_raw_data_path = os.environ.get("GCS_RAW_DATA_PATH")
log_dir = project_root / os.environ.get("LOG_DIR")
key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
bq_dataset = os.environ.get("BQ_DATASET")

# Logging
timestamp = datetime.now().strftime("%Y%m%d-%H%M")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = os.path.join(log_dir, f"03_gcs_to_fhir_store_to_bq_{timestamp}.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Get the latest folder from GCS
try:
    folder_list = subprocess.check_output(
        ["gsutil", "ls", gcs_raw_data_path], text=True
    ).splitlines()
    timestamp_folders = [
        f.rstrip("/").split("/")[-1]
        for f in folder_list
        if f.rstrip("/").split("/")[-1][0].isdigit()
    ]
    latest_folder = sorted(timestamp_folders)[-1]
    gcs_input_path = f"{gcs_raw_data_path}/{latest_folder}/fhir/*.json"
    logging.info(f"Latest timestamped folder: {latest_folder}")
    logging.info(f"Reading from: {gcs_input_path}")
except Exception as e:
    logging.error(f"Error listing GCS folders: {str(e)}")
    raise

# Set up Healthcare API client
credentials = service_account.Credentials.from_service_account_file(key_path)
service = discovery.build("healthcare", "v1", credentials=credentials)

# Build request body
parent = f"projects/{project_id}/locations/{location}/datasets/{dataset_id}/fhirStores/{fhir_store_id}"
# body = {"gcsSource": {"uri": "gs://healthcare_elt_bucket/temp/cleaned.json"}}
body = {"gcsSource": {"uri": gcs_input_path}}

logging.info(f"Importing files from: {gcs_input_path} into FHIR Store: {parent}")
logging.info("Initiating FHIR import from GCS to FHIR Store.")

# Execute import request
try:
    request = (
        service.projects()
        .locations()
        .datasets()
        .fhirStores()
        .import_(name=parent, body=body)
    )
    response = request.execute()
    logging.info(f"Import started: {response}")
    print("Import started:", response)
except Exception as e:
    logging.error(f"FHIR import failed: {str(e)}")
    raise

# Check import status
operation_name = response["name"]
operation = (
    service.projects().locations().datasets().operations().get(name=operation_name)
)
while True:
    result = operation.execute()
    if result.get("done"):
        if "error" in result:
            logging.error(f"Import failed: {result['error']['message']}")
            print("Import failed:", result["error"]["message"])
        else:
            logging.info("Import completed successfully.")
            print("Import completed successfully.")
        break
    else:
        logging.info("Waiting for import operation to complete...")
        time.sleep(5)
