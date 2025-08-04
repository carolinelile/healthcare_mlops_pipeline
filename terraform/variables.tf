
variable "project_id" {
  description = "GCP Project ID"
  default = "healthcare-elt-pipeline"
}

variable "project_name" {
  description = "GCP Project name"
  default     = "Healthcare ELT Pipeline"
}

variable "region" {
  description = "GCP Region"
  default     = "us-central1"
}

variable "zone" {
  description = "GCP Zone"
  default     = "us-central1-a"
}

variable "gcs_bucket_name" {
  description = "GCS bucket name for raw FHIR data"
  default     = "healthcare_elt_bucket"
}

variable "bq_dataset_id" {
  description = "BigQuery dataset for healthcare data"
  default     = "healthcare_dataset"
}
