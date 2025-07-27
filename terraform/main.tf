terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.41.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "data-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  storage_class = "STANDARD"
  uniform_bucket_level_access = true


  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }
  
}

resource "google_bigquery_dataset" "dataset_stg" {
  dataset_id = var.bq_dataset_name_1
  project = var.project
  location   = var.location
}

resource "google_bigquery_dataset" "dataset_fact" {
  dataset_id = var.bq_dataset_name_2
  project = var.project
  location   = var.location
}