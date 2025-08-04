import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import aiplatform

# === Load environment variables ===
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

PROJECT_ID = os.environ["PROJECT_ID"]
REGION = os.environ["LOCATION"]
MODEL_NAME = os.environ["MODEL_NAME"] 
BQ_SOURCE_URI = os.environ["BQ_SOURCE_URI"] 
BQ_OUTPUT_URI = os.environ["BQ_PREDICTION_OUTPUT"]  
JOB_DISPLAY_NAME = f"batch-predict-{datetime.now().strftime('%Y%m%d-%H%M')}"

# === Initialize Vertex AI ===
aiplatform.init(project=PROJECT_ID, location=REGION)

def run_batch_prediction():
    model = aiplatform.Model.list(filter=f"display_name={MODEL_NAME}")[0]

    batch_job = model.batch_predict(
        job_display_name=JOB_DISPLAY_NAME,
        bigquery_source=BQ_SOURCE_URI,
        bigquery_destination_output_uri=BQ_OUTPUT_URI,
        instances_format="bigquery",
        predictions_format="bigquery",
        machine_type="n1-standard-4",
        sync=True,
    )

    print(f"✅ Batch prediction complete. Output: {BQ_OUTPUT_URI}")

if __name__ == "__main__":
    run_batch_prediction()
