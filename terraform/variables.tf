variable "credentials" {
  description = "My Credentials"
  default     = "./../credentials.json"
}

variable "project" {
  description = "Project"
  default     = "earthquake-data-467123"
}

variable "region" {
  description = "Region"
  default     = "us-west1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name_1" {
  description = "Earthquake Data Staging Dataset Name"
  default     = "earthquake_stg_data"
}

variable "bq_dataset_name_2" {
  description = "Earthquake Data Processed Dataset Name"
  default     = "earthquake_data_fact"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "earthquake_data_buckets"
}