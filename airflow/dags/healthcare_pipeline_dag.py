from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 1),
    "retries": 1,
}

with DAG(
    dag_id="healthcare_mlops_pipeline",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=False,
    tags=["healthcare", "mlops"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_synthea_data",
        bash_command="bash /opt/airflow/scripts/01_generate_synthea_data.sh",
    )

    ingest_to_gcs = PythonOperator(
        task_id="ingest_fhir_to_gcs",
        python_callable=lambda: exec(open("/opt/airflow/scripts/02_ingest_to_gcs.py").read()),
    )

    load_to_fhir = PythonOperator(
        task_id="load_to_fhir_store",
        python_callable=lambda: exec(open("/opt/airflow/scripts/03_load_to_fhir_store.py").read()),
    )

    export_to_bq = PythonOperator(
        task_id="export_fhir_to_bq",
        python_callable=lambda: exec(open("/opt/airflow/scripts/04_export_fhir_to_bq.py").read()),
    )

    train_model = PythonOperator(
        task_id="train_model",
        python_callable=lambda: exec(open("/opt/airflow/scripts/05_train_model.py").read()),
    )

    batch_predict = PythonOperator(
        task_id="batch_prediction",
        python_callable=lambda: exec(open("/opt/airflow/scripts/06_batch_predict.py").read()),
    )

    monitor_retrain = PythonOperator(
        task_id="monitor_and_retrain",
        python_callable=lambda: exec(open("/opt/airflow/scripts/07_monitor_and_retrain.py").read()),
    )

    generate_data >> ingest_to_gcs >> load_to_fhir >> export_to_bq >> train_model >> batch_predict >> monitor_retrain
