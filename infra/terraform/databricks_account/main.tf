# OPTIONAL. Only needed if you do not already have a Databricks workspace on AWS.
# Creates: cross-account IAM role (Databricks launches EC2 in your account with it), workspace root bucket, and the
# workspace itself. Needs account-level OAuth credentials (account console -> service principal -> secret).
#
# Apply this BEFORE ../ (the main stack), then set databricks_host / databricks_token there from the outputs.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 5.60" }
    databricks = { source = "databricks/databricks", version = "~> 1.50" }
  }
}

variable "region" {
  type    = string
  default = "eu-west-2"
}
variable "databricks_account_id" { type = string }
variable "databricks_client_id" { type = string }
variable "databricks_client_secret" {
  type      = string
  sensitive = true
}
variable "workspace_name" {
  type    = string
  default = "glenergy-dev"
}
variable "cluster_role_arn" {
  description = "ARN of the instance-profile role created by the main stack (output databricks_cluster_role_arn). It can be predicted: arn:aws:iam::<account>:role/glenergy-dev-databricks-cluster"
  type        = string
}

provider "aws" { region = var.region }

provider "databricks" {
  alias         = "mws"
  host          = "https://accounts.cloud.databricks.com"
  account_id    = var.databricks_account_id
  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
}

data "databricks_aws_assume_role_policy" "this" {
  provider    = databricks.mws
  external_id = var.databricks_account_id
}

resource "aws_iam_role" "cross_account" {
  name               = "${var.workspace_name}-databricks-cross-account"
  assume_role_policy = data.databricks_aws_assume_role_policy.this.json
}

data "databricks_aws_crossaccount_policy" "this" {
  provider   = databricks.mws
  pass_roles = [var.cluster_role_arn] # lets Databricks attach the instance profile to clusters
}

resource "aws_iam_role_policy" "cross_account" {
  role   = aws_iam_role.cross_account.id
  policy = data.databricks_aws_crossaccount_policy.this.json
}

resource "databricks_mws_credentials" "this" {
  provider         = databricks.mws
  credentials_name = "${var.workspace_name}-creds"
  role_arn         = aws_iam_role.cross_account.arn
  depends_on       = [aws_iam_role_policy.cross_account]
}

resource "aws_s3_bucket" "root" {
  bucket        = "${var.workspace_name}-dbx-root-${data.aws_caller_identity.this.account_id}"
  force_destroy = true
}

data "aws_caller_identity" "this" {}

resource "aws_s3_bucket_public_access_block" "root" {
  bucket                  = aws_s3_bucket.root.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "databricks_aws_bucket_policy" "root" {
  provider = databricks.mws
  bucket   = aws_s3_bucket.root.bucket
}

resource "aws_s3_bucket_policy" "root" {
  bucket     = aws_s3_bucket.root.id
  policy     = data.databricks_aws_bucket_policy.root.json
  depends_on = [aws_s3_bucket_public_access_block.root]
}

resource "databricks_mws_storage_configurations" "this" {
  provider                   = databricks.mws
  account_id                 = var.databricks_account_id
  storage_configuration_name = "${var.workspace_name}-storage"
  bucket_name                = aws_s3_bucket.root.bucket
}

resource "databricks_mws_workspaces" "this" {
  provider                 = databricks.mws
  account_id               = var.databricks_account_id
  aws_region               = var.region
  workspace_name           = var.workspace_name
  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
  token {}
  depends_on = [aws_s3_bucket_policy.root]
}

output "databricks_host" { value = databricks_mws_workspaces.this.workspace_url }
output "databricks_token" {
  value     = databricks_mws_workspaces.this.token[0].token_value
  sensitive = true
}
