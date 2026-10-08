terraform {
  required_version = ">= 1.6"
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 5.60" }
    databricks = { source = "databricks/databricks", version = "~> 1.50" }
    random     = { source = "hashicorp/random", version = "~> 3.6" }
    archive    = { source = "hashicorp/archive", version = "~> 2.4" }
  }
  # Local state for the MVP. Move to S3 + DynamoDB locking before more than one person applies.
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "global-energy-lakehouse", env = var.env, managed_by = "terraform" }
  }
}

# The workspace must already exist (create it in the Databricks account console, or with ./databricks_account).
provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
