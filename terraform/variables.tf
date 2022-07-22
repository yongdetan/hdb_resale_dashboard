locals {
  data_lake_bucket = "data_lake"
}

variable "project" {
  default = "resale-market"
}

variable "region" {
  default = "asia-southeast1"
  type = string
}

variable "storage_class" {
  default = "STANDARD"
}

variable "BQ_DATASET" {
  type = string
  default = "hdb_resale_market"
}