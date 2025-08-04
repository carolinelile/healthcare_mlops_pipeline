import os
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from google.cloud import bigquery
from google.cloud import aiplatform
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import pandas as pd
import mlflow
import mlflow.sklearn
import joblib

# === Load env vars ===
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

PROJECT_ID = os.environ["PROJECT_ID"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE = os.environ["BQ_ML_TABLE"] 
REGION = os.environ["LOCATION"]
MODEL_NAME = "no_show_predictor"
MODEL_DIR = "models"

# === Logging ===
timestamp = datetime.now().strftime("%Y%m%d-%H%M")
log_dir = project_root / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_path = log_dir / f"05_train_model_{timestamp}.log"
logging.basicConfig(filename=log_path, level=logging.INFO)

# === BigQuery Client ===
bq_client = bigquery.Client(project=PROJECT_ID)

# === Vertex AI Init ===
aiplatform.init(project=PROJECT_ID, location=REGION)

# === MLflow Init ===
mlflow.set_tracking_uri("file://" + str(project_root / "mlruns"))
mlflow.set_experiment("no_show_prediction")

def load_data():
    query = f"SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` WHERE label IS NOT NULL"
    df = bq_client.query(query).to_dataframe()
    return df

def preprocess(df):
    df = df.dropna()
    X = df.drop(columns=["label"])
    y = df["label"]
    return train_test_split(X, y, test_size=0.2, random_state=42)

def train_and_log(X_train, X_test, y_train, y_test):
    with mlflow.start_run():
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        report = classification_report(y_test, preds, output_dict=True)
        mlflow.log_metrics({k: v for k, v in report["weighted avg"].items() if isinstance(v, float)})

        model_path = Path(MODEL_DIR) / f"{MODEL_NAME}_{timestamp}.joblib"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, model_path)

        mlflow.sklearn.log_model(model, artifact_path="model")
        return model_path

def upload_to_vertex_ai(model_path):
    model = aiplatform.Model.upload(
        display_name=MODEL_NAME,
        artifact_uri=str(model_path.parent.resolve()),
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest"
    )
    logging.info(f"Model uploaded to Vertex AI: {model.resource_name}")
    return model

if __name__ == "__main__":
    df = load_data()
    X_train, X_test, y_train, y_test = preprocess(df)
    model_path = train_and_log(X_train, X_test, y_train, y_test)
    upload_to_vertex_ai(model_path)
    print("Model training and upload complete.")
