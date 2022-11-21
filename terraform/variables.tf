variable "environment" {
  type        = string
  description = "An environment namespace for the infrastructure."
}

variable "data_api_url" {
  type        = string
  description = "Environment specific Data API URL"
}

variable "stac_bucket" {
  type        = string
  description = "AWS S3 bucket where STAC Catalog is stored."
}

variable "data_api_key" {
  type        = string
  description = "API key needed to download assets"
}
