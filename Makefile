# Makefile

.PHONY: help setup lint test clean run-all

help:  ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?##' Makefile | sort | awk 'BEGIN {FS = ":.*?##"}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup:  ## Install Python dependencies
	pip install -r requirements.txt

lint:  ## Lint and format code using black and flake8
	black . && flake8 .

test:  ## Run unit tests
	pytest tests/

clean:  ## Delete all .pyc files and __pycache__
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -r {} +

run-all:  ## Run all scripts in sequence
	python scripts/01_generate_synthea_data.py
	python scripts/02_ingest_fhir_to_gcs.py
	python scripts/03_gcs_to_fhir_store.py
	python scripts/04_export_fhir_to_bq.py
	python scripts/05_train_model.py
	python scripts/06_batch_predict.py
	python scripts/07_monitor_and_retrain.py
