# Glue = catalog (Iceberg tables written by Databricks register here, Athena reads them) + contract check job.

resource "aws_glue_catalog_database" "layers" {
  for_each     = toset(["bronze", "silver", "gold", "ctl", "spike"])
  name         = each.key
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/warehouse/${each.key}.db/"
}

data "archive_file" "lakehouse" {
  type        = "zip"
  output_path = "${path.module}/.build/lakehouse.zip"
  dynamic "source" {
    for_each = fileset("${path.module}/../../lakehouse", "**/*.py")
    content {
      content  = file("${path.module}/../../lakehouse/${source.value}")
      filename = "lakehouse/${source.value}"
    }
  }
}

resource "aws_s3_object" "lakehouse_zip" {
  bucket = aws_s3_bucket.lake.id
  key    = "artifacts/lakehouse.zip"
  source = data.archive_file.lakehouse.output_path
  etag   = data.archive_file.lakehouse.output_md5
}

resource "aws_s3_object" "contract_check" {
  bucket = aws_s3_bucket.lake.id
  key    = "artifacts/contract_check.py"
  source = "${path.module}/../../glue/contract_check.py"
  etag   = filemd5("${path.module}/../../glue/contract_check.py")
}

resource "aws_glue_job" "contract_check" {
  name              = "${local.name}-contract-check"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.contract_check.key}"
    python_version  = "3"
  }
  default_arguments = {
    "--extra-py-files"                   = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.lakehouse_zip.key}"
    "--additional-python-modules"        = "pyarrow"
    "--bucket"                           = aws_s3_bucket.lake.bucket
    "--enable-continuous-cloudwatch-log" = "true"
  }
}

# ---- Athena
resource "aws_athena_workgroup" "this" {
  name          = local.name
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    engine_version { selected_engine_version = "Athena engine version 3" }
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
    }
  }
}
