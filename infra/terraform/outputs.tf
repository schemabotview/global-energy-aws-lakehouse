output "lake_bucket" { value = aws_s3_bucket.lake.bucket }
output "landing_uri" { value = "s3://${aws_s3_bucket.lake.bucket}/dms/src" }
output "rds_endpoint" { value = aws_db_instance.src.address }
output "rds_secret_arn" { value = aws_secretsmanager_secret.db.arn }
output "dms_task_arn" { value = aws_dms_replication_task.this.replication_task_arn }
output "athena_workgroup" { value = aws_athena_workgroup.this.name }
output "state_machine_arn" { value = aws_sfn_state_machine.pipeline.arn }
output "databricks_job_id" { value = databricks_job.pipeline.id }
output "databricks_cluster_role_arn" {
  description = "Add this to iam:PassRole in the Databricks cross-account role (see databricks_account/)."
  value       = aws_iam_role.databricks_cluster.arn
}
