terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}
 
provider "google" {
  #   credentials = file(var.credentials) # if using the credentials variable rather than the env-var GOOGLE_APPLICATION_CREDENTIALS
  project = var.project
  region  = var.location
}


resource "google_storage_bucket" "project-bucket" {
  name          = "${var.gcs_bucket_name}_${var.project}"
  location = var.region
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}



resource "google_bigquery_dataset" "project-dataset" {
  dataset_id = var.bq_dataset_name
  location = var.region
}

# resource "google_bigquery_table" "external_table" {
#   dataset_id = var.bq_dataset_name
#   table_id   = "raw_daily_external"

#   external_data_configuration {
#     autodetect    = true
#     source_format = "PARQUET"

#     source_uris = [
#       "gs://${local.data_lake_bucket}_${var.project}/*.parquet",
#     ]
#   }
# }