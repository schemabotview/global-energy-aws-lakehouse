# RDS PostgreSQL --(DMS full load + CDC)--> s3://<bucket>/dms/src/<table>/...  (parquet, Op column, commit timestamp)
#
# After `terraform apply`: reboot the RDS instance once so rds.logical_replication takes effect, seed the source
# (python -m generator.cli postgres --day 1 --reset), then start the task (see README).

resource "aws_dms_replication_subnet_group" "this" {
  replication_subnet_group_id          = "${local.name}-dms"
  replication_subnet_group_description = "DMS subnets"
  subnet_ids                           = aws_subnet.public[*].id
  depends_on                           = [aws_iam_role_policy_attachment.dms_vpc]
}

resource "aws_dms_replication_instance" "this" {
  replication_instance_id     = "${local.name}-dms"
  replication_instance_class  = "dms.t3.micro"
  allocated_storage           = 20
  replication_subnet_group_id = aws_dms_replication_subnet_group.this.id
  vpc_security_group_ids      = [aws_security_group.dms.id]
  publicly_accessible         = false
  multi_az                    = false
  apply_immediately           = true
}

resource "aws_dms_endpoint" "source" {
  endpoint_id   = "${local.name}-src-postgres"
  endpoint_type = "source"
  engine_name   = "postgres"
  server_name   = aws_db_instance.src.address
  port          = aws_db_instance.src.port
  database_name = aws_db_instance.src.db_name
  username      = aws_db_instance.src.username
  password      = random_password.db.result
  ssl_mode      = "require"
  # test_decoding is available on RDS without extra extensions.
  extra_connection_attributes = "PluginName=test_decoding;heartbeatEnable=Y;heartbeatFrequency=1"
}

resource "aws_dms_s3_endpoint" "target" {
  endpoint_id             = "${local.name}-s3-target"
  endpoint_type           = "target"
  bucket_name             = aws_s3_bucket.lake.bucket
  bucket_folder           = "dms"
  service_access_role_arn = aws_iam_role.dms_s3.arn

  data_format                     = "parquet"
  parquet_version                 = "parquet-2-0"
  parquet_timestamp_in_millisecond = false
  include_op_for_full_load        = true      # Op column on full-load rows too (value I)
  timestamp_column_name           = "dms_commit_ts"
  date_partition_enabled       = true      # CDC files land under <table>/YYYY/MM/DD/
  cdc_max_batch_interval          = 60
  cdc_min_file_size               = 32000
  encryption_mode                 = "SSE_S3"
}

resource "aws_dms_replication_task" "this" {
  replication_task_id      = "${local.name}-pg-to-s3"
  migration_type           = "full-load-and-cdc"
  replication_instance_arn = aws_dms_replication_instance.this.replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.source.endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.target.endpoint_arn
  start_replication_task   = false # start it after the source is seeded and RDS has been rebooted

  table_mappings = jsonencode({
    rules = [{
      "rule-type"      = "selection"
      "rule-id"        = "1"
      "rule-name"      = "all-src-tables"
      "object-locator" = { "schema-name" = "src", "table-name" = "%" }
      "rule-action"    = "include"
    }]
  })
  replication_task_settings = jsonencode({
    Logging = { EnableLogging = true }
    FullLoadSettings = { TargetTablePrepMode = "DO_NOTHING" }
  })
}
