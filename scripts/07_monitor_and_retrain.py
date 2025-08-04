import os
from google.cloud import bigquery
from google.cloud import aiplatform
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# === Load environment variables ===
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

PROJECT_ID = os.environ["PROJECT_ID"]
LOCATION = os.environ["LOCATION"]
BQ_PREDICTION_TABLE = os.environ["BQ_PREDICTION_TABLE"]  
THRESHOLD_LOW_CONFIDENCE_RATIO = 0.3  
MODEL_DISPLAY_NAME = os.environ["MODEL_NAME"]

client = bigquery.Client(project=PROJECT_ID)

def check_for_drift():
    query = f"""
    SELECT
      COUNT(*) AS total_predictions,
      SUM(CASE WHEN confidence < 0.6 THEN 1 ELSE 0 END) AS low_confidence_count
    FROM `{PROJECT_ID}.{BQ_PREDICTION_TABLE}`
    """

    result = client.query(query).result().to_dataframe()
    total = result['total_predictions'][0]
    low_conf = result['low_confidence_count'][0]

    if total == 0:
        print("⚠️ No predictions found.")
        return False

    ratio = low_conf / total
    print(f"🔍 Drift check: {low_conf}/{total} = {ratio:.2%} low confidence")

    return ratio > THRESHOLD_LOW_CONFIDENCE_RATIO

def trigger_retraining():
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    job_display_name = f"retrain-no-show-model-{timestamp}"

    aiplatform.init(project=PROJECT_ID, location=LOCATION)

    training_job = aiplatform.CustomPythonPackageTrainingJob(
        display_name=job_display_name,
        python_package_gcs_uri=os.environ["TRAINING_PKG_GCS_PATH"],
        python_module_name="train_model", 
        container_uri="us-docker.pkg.dev/vertex-ai/training/scikit-learn-cpu.0-24:latest",
    )

    training_job.run(
        args=[],
        replica_count=1,
        model_display_name=MODEL_DISPLAY_NAME,
        base_output_dir=os.environ["MODEL_OUTPUT_GCS_DIR"],
    )

    print(f"✅ Retraining triggered: {job_display_name}")

if __name__ == "__main__":
    if check_for_drift():
        print("🚨 Drift detected. Triggering retraining...")
        trigger_retraining()
    else:
        print("✅ No significant drift detected.")
