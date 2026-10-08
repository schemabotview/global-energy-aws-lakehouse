# Step Functions runs: Glue contract check -> Databricks job -> SNS alert on failure.
# The Databricks Jobs API is called with the Step Functions HTTP task through an EventBridge connection.

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_event_connection" "databricks" {
  name               = "${local.name}-databricks"
  authorization_type = "API_KEY"
  auth_parameters {
    api_key {
      key   = "Authorization"
      value = "Bearer ${var.databricks_token}"
    }
  }
}

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${local.name}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

resource "aws_iam_role_policy" "sfn" {
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"], Resource = aws_glue_job.contract_check.arn },
      { Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.alerts.arn },
      { Effect = "Allow", Action = ["states:InvokeHTTPEndpoint"], Resource = "*" },
      { Effect = "Allow", Action = ["events:RetrieveConnectionCredentials"], Resource = aws_cloudwatch_event_connection.databricks.arn },
      { Effect = "Allow", Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"], Resource = aws_cloudwatch_event_connection.databricks.secret_arn },
      {
        Effect   = "Allow"
        Action   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
        Resource = "arn:aws:events:${var.region}:${data.aws_caller_identity.this.account_id}:rule/StepFunctionsGetEventsForGlueJobsRule"
      },
    ]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name}-pipeline"
  role_arn = aws_iam_role.sfn.arn
  definition = templatefile("${path.module}/../../orchestration/state_machine.asl.json", {
    glue_job_name     = aws_glue_job.contract_check.name
    databricks_host   = var.databricks_host
    databricks_job_id = databricks_job.pipeline.id
    connection_arn    = aws_cloudwatch_event_connection.databricks.arn
    topic_arn         = aws_sns_topic.alerts.arn
  })
}

# Daily trigger (off by default). The EventBridge event id becomes the run_id.
resource "aws_cloudwatch_event_rule" "daily" {
  name                = "${local.name}-daily"
  schedule_expression = var.schedule_expression
  state               = var.schedule_enabled ? "ENABLED" : "DISABLED"
}

data "aws_iam_policy_document" "events_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events" {
  name               = "${local.name}-events"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
}

resource "aws_iam_role_policy" "events" {
  role = aws_iam_role.events.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["states:StartExecution"], Resource = aws_sfn_state_machine.pipeline.arn }]
  })
}

resource "aws_cloudwatch_event_target" "daily" {
  rule     = aws_cloudwatch_event_rule.daily.name
  arn      = aws_sfn_state_machine.pipeline.arn
  role_arn = aws_iam_role.events.arn
  input_transformer {
    input_paths    = { id = "$.id" }
    input_template = "{\"run_id\": \"<id>\"}"
  }
}
