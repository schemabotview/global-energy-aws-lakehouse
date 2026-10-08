variable "region" {
  type    = string
  default = "eu-west-2"
}
variable "env" {
  type    = string
  default = "dev"
}
variable "name_prefix" {
  type    = string
  default = "glenergy"
}
variable "admin_cidrs" {
  description = "CIDR blocks allowed to reach RDS on 5432 (your laptop / CI egress) so the generator can seed it."
  type        = list(string)
}
variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}
variable "create_dms_service_roles" {
  description = "dms-vpc-role and dms-cloudwatch-logs-role are account-wide singletons. Set false if they already exist."
  type        = bool
  default     = true
}
variable "alert_email" {
  description = "Receives pipeline failure notifications (confirm the SNS subscription email)."
  type        = string
}
variable "schedule_enabled" {
  type    = bool
  default = false
}
variable "schedule_expression" {
  type    = string
  default = "cron(30 3 * * ? *)"
}

# --- Databricks (workspace level)
variable "databricks_host" {
  description = "e.g. https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"
  type        = string
}
variable "databricks_token" {
  description = "Token of a service principal (or PAT) that can create jobs. Also used by Step Functions via an EventBridge connection."
  type        = string
  sensitive   = true
}
variable "git_url" {
  description = "Repo the job notebooks are read from (Git job source). Private repos need a Git credential in the workspace."
  type        = string
  default     = "https://github.com/schemabotview/global-energy-aws-lakehouse"
}
variable "git_branch" {
  type    = string
  default = "main"
}
variable "node_type_id" {
  type    = string
  default = "m5d.large"
}
variable "data_security_mode" {
  description = "NONE (no isolation shared) is the simplest mode for an instance profile + open-source Iceberg catalog."
  type        = string
  default     = "NONE"
}
variable "iceberg_version" {
  type    = string
  default = "1.6.1"
}
