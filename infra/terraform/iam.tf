# ---- DMS service roles (account-wide singletons with fixed names)
data "aws_iam_policy_document" "dms_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["dms.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "dms_vpc" {
  count              = var.create_dms_service_roles ? 1 : 0
  name               = "dms-vpc-role"
  assume_role_policy = data.aws_iam_policy_document.dms_assume.json
}
resource "aws_iam_role_policy_attachment" "dms_vpc" {
  count      = var.create_dms_service_roles ? 1 : 0
  role       = aws_iam_role.dms_vpc[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
}
resource "aws_iam_role" "dms_logs" {
  count              = var.create_dms_service_roles ? 1 : 0
  name               = "dms-cloudwatch-logs-role"
  assume_role_policy = data.aws_iam_policy_document.dms_assume.json
}
resource "aws_iam_role_policy_attachment" "dms_logs" {
  count      = var.create_dms_service_roles ? 1 : 0
  role       = aws_iam_role.dms_logs[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSCloudWatchLogsRole"
}

# ---- DMS -> S3 target role
resource "aws_iam_role" "dms_s3" {
  name               = "${local.name}-dms-s3"
  assume_role_policy = data.aws_iam_policy_document.dms_assume.json
}
resource "aws_iam_role_policy" "dms_s3" {
  role = aws_iam_role.dms_s3.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:PutObject", "s3:DeleteObject", "s3:PutObjectTagging"], Resource = "${aws_s3_bucket.lake.arn}/dms/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.lake.arn },
    ]
  })
}

# ---- Databricks cluster role (instance profile): S3 lake bucket + Glue Catalog
data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "databricks_cluster" {
  name               = "${local.name}-databricks-cluster"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}
resource "aws_iam_role_policy" "databricks_cluster" {
  role = aws_iam_role.databricks_cluster.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], Resource = "${aws_s3_bucket.lake.arn}/*" },
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"], Resource = aws_s3_bucket.lake.arn },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase", "glue:UpdateDatabase",
          "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.this.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.this.account_id}:database/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.this.account_id}:table/*/*",
        ]
      },
    ]
  })
}
resource "aws_iam_instance_profile" "databricks_cluster" {
  name = "${local.name}-databricks-cluster"
  role = aws_iam_role.databricks_cluster.name
}

# ---- Glue job role (contract check)
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
resource "aws_iam_role_policy" "glue_s3" {
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], Resource = "${aws_s3_bucket.lake.arn}/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.lake.arn },
    ]
  })
}
