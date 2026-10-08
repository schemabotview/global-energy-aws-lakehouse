# Workspace-level resources. The cluster reaches S3 + Glue through an instance profile (see iam.tf) and runs the
# open-source Iceberg runtime with GlueCatalog, so Databricks writes the same tables Athena reads.

locals {
  libs = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${var.iceberg_version}",
    "org.apache.iceberg:iceberg-aws-bundle:${var.iceberg_version}",
  ]

  spark_conf = {
    "spark.sql.extensions"                        = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.lake"                      = "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.lake.catalog-impl"         = "org.apache.iceberg.aws.glue.GlueCatalog"
    "spark.sql.catalog.lake.warehouse"            = "s3://${aws_s3_bucket.lake.bucket}/warehouse"
    "spark.sql.catalog.lake.io-impl"              = "org.apache.iceberg.aws.s3.S3FileIO"
    "spark.sql.session.timeZone"                  = "UTC"
    "spark.sql.parquet.inferTimestampNTZ.enabled" = "false"
    "spark.databricks.cluster.profile"            = "singleNode"
    "spark.master"                                = "local[*, 4]"
  }

  # task_key => notebook, upstream tasks, retries. Retries are for infrastructure hiccups only: a DQ or
  # reconciliation failure is a real result and is not retried.
  tasks = {
    setup             = { notebook = "00_setup", after = [], retries = 1 }
    bronze            = { notebook = "10_bronze_load", after = ["setup"], retries = 1 }
    silver            = { notebook = "20_silver", after = ["bronze"], retries = 1 }
    gold_dims         = { notebook = "30_gold_dims", after = ["silver"], retries = 1 }
    gold_facts        = { notebook = "40_gold_facts", after = ["gold_dims"], retries = 1 }
    dq                = { notebook = "50_dq", after = ["gold_facts"], retries = 0 }
    reconcile_publish = { notebook = "60_reconcile_publish", after = ["dq"], retries = 0 }
  }
}

resource "databricks_instance_profile" "this" {
  instance_profile_arn = aws_iam_instance_profile.databricks_cluster.arn
}

resource "databricks_job" "pipeline" {
  name                = "${local.name}-pipeline"
  max_concurrent_runs = 1

  git_source {
    url      = var.git_url
    provider = "gitHub"
    branch   = var.git_branch
  }

  parameter {
    name    = "run_id"
    default = "manual"
  }
  parameter {
    name    = "landing_uri"
    default = "s3://${aws_s3_bucket.lake.bucket}/dms/src"
  }
  parameter {
    name    = "ctl_uri"
    default = "s3://${aws_s3_bucket.lake.bucket}/ctl"
  }
  parameter {
    name    = "quarantine_uri"
    default = "s3://${aws_s3_bucket.lake.bucket}/quarantine"
  }
  parameter {
    name    = "recon_threshold_pct"
    default = "0.01"
  }

  job_cluster {
    job_cluster_key = "main"
    new_cluster {
      spark_version      = "15.4.x-scala2.12"
      node_type_id       = var.node_type_id
      num_workers        = 0
      data_security_mode = var.data_security_mode
      spark_conf         = local.spark_conf
      custom_tags        = { ResourceClass = "SingleNode" }
      spark_env_vars     = { AWS_REGION = var.region }
      aws_attributes {
        instance_profile_arn = databricks_instance_profile.this.instance_profile_arn
        availability         = "SPOT_WITH_FALLBACK"
        first_on_demand      = 1
        zone_id              = "auto"
      }
    }
  }

  dynamic "task" {
    for_each = local.tasks
    content {
      task_key                  = task.key
      job_cluster_key           = "main"
      max_retries               = task.value.retries
      min_retry_interval_millis = 60000

      notebook_task {
        notebook_path = "databricks/notebooks/${task.value.notebook}"
        source        = "GIT"
        base_parameters = {
          run_id              = "{{job.parameters.run_id}}"
          landing_uri         = "{{job.parameters.landing_uri}}"
          ctl_uri             = "{{job.parameters.ctl_uri}}"
          quarantine_uri      = "{{job.parameters.quarantine_uri}}"
          recon_threshold_pct = "{{job.parameters.recon_threshold_pct}}"
        }
      }

      dynamic "depends_on" {
        for_each = task.value.after
        content {
          task_key = depends_on.value
        }
      }

      dynamic "library" {
        for_each = local.libs
        content {
          maven { coordinates = library.value }
        }
      }
    }
  }
}
