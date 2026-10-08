locals {
  name   = "${var.name_prefix}-${var.env}"
  bucket = "${var.name_prefix}-${var.env}-lake-${data.aws_caller_identity.this.account_id}"
}

# One bucket, prefixes: dms/ (raw DMS output), warehouse/ (Iceberg), ctl/, quarantine/, artifacts/, athena-results/
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = true # MVP / dev only
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

# Raw DMS files are kept 90 days in S3, then Glacier (as in the design guide).
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "dms-raw-to-glacier"
    status = "Enabled"
    filter { prefix = "dms/" }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
  rule {
    id     = "athena-results-expire"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 14 }
  }
}
