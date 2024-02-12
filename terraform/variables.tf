locals {
  data_lake_bucket = "london_property_data"
}


variable "credentials" {
  description = "My Credentials"
  default     = "<Path to your Service Account json file>"
  # ex: if you have a directory where this file is called keys with your service account json file
  # saved there as my-creds.json you could use default = "./keys/my-creds.json"
  # Use this variable if you aren't using the env-var GOOGLE_APPLICATION_CREDENTIALS
}
 

variable "project" {
  description = "Project"
  default     = "evident-display-410312"
}

variable "region" {
  description = "Region for GCP resources"
  #Update the below to your desired region
  default = "europe-west2"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "EU"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "properties_dataset"
}

variable "gcs_bucket_name" {
  description = "Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "london_property_data"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}