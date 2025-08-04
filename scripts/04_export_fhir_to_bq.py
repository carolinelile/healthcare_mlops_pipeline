import os
import json
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient import discovery

# Load environment variables
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent
env_path = project_root / ".env"
if not env_path.exists():
    raise FileNotFoundError(f".env file not found at {env_path}")
load_dotenv(dotenv_path=env_path)

PROJECT_ID = os.environ["PROJECT_ID"]
LOCATION = os.environ["LOCATION"]
DATASET_ID = os.environ["DATASET_ID"]
FHIR_STORE_ID = os.environ["FHIR_STORE_ID"]
BQ_DATASET = os.environ["BQ_DATASET"]
CREDENTIALS_PATH = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
LOG_DIR = project_root / os.environ.get("LOG_DIR", "logs")

# Set up logging
timestamp = datetime.now().strftime("%Y%m%d-%H%M")
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / f"04_export_fhir_to_bq_{timestamp}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Authenticate and create API client
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
service = discovery.build("healthcare", "v1", credentials=credentials)

def export_fhir_to_bq():
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}/datasets/{DATASET_ID}/fhirStores/{FHIR_STORE_ID}"
    export_uri = f"bq://{PROJECT_ID}.{BQ_DATASET}"

    body = {
        "bigqueryDestination": {
            "datasetUri": export_uri,
            "schemaConfig": {
                "schemaType": "ANALYTIC_SCHEMA"
            }
        }
    }

    request = service.projects().locations().datasets().fhirStores().export(name=parent, body=body)
    try:
        response = request.execute()
        logging.info(f"FHIR export to BigQuery started: {response}")
        print("Export started. Operation details:", response)
    except Exception as e:
        logging.error(f"Error during FHIR export: {str(e)}")
        print("Export failed:", str(e))

if __name__ == "__main__":
    export_fhir_to_bq()
