
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Create a service account for the VM and data access
resource "google_service_account" "elt_service_account" {
  account_id   = "elt-service-account"
  display_name = "ELT Service Account"
}

# Grant roles to the service account
resource "google_project_iam_member" "elt_sa_gcs_access" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.elt_service_account.email}"
}

resource "google_storage_bucket_iam_member" "elt_sa_gcs_admin" {
  bucket = google_storage_bucket.healthcare_elt_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.elt_service_account.email}"
}

resource "google_project_iam_member" "elt_sa_bq_access" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.elt_service_account.email}"
}

resource "google_project_iam_member" "elt_sa_compute" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.elt_service_account.email}"
}

resource "google_project_iam_member" "elt_sa_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.elt_service_account.email}"
}

resource "google_project_iam_member" "elt_sa_monitoring" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.elt_service_account.email}"
}

# Create a GCS bucket
resource "google_storage_bucket" "healthcare_elt_bucket" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Create a VM with startup script to install Spark, dbt, Airflow
resource "google_compute_instance" "elt_vm" {
  name         = "elt-vm"
  machine_type = "n1-standard-2"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 50
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata_startup_script = file("startup.sh")

  service_account {
    email  = google_service_account.elt_service_account.email
    scopes = ["cloud-platform"]
  }

  tags = ["elt", "spark", "airflow", "dbt"]
}

# Create a BigQuery dataset
resource "google_bigquery_dataset" "healthcare_dataset" {
  dataset_id = var.bq_dataset_id
  location   = var.region
  project    = var.project_id
}
