resource "aws_glue_catalog_database" "commerce" {
  name        = replace(local.name, "-", "_")
  description = "Explicit schemas for committed synthetic commerce snapshots; no inferred crawler contracts."
}
resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr-serverless/${local.name}"
  retention_in_days = 14
}
resource "aws_iam_role" "emr_job" {
  name = "${local.name}-emr-job"
  assume_role_policy = jsonencode({
    Version = "2012-10-17", Statement = [{
      Effect = "Allow", Principal = { Service = "emr-serverless.amazonaws.com" }, Action = "sts:AssumeRole"
      Condition = {
        StringEquals = { "aws:SourceAccount" = local.account_id }
        ArnLike      = { "aws:SourceArn" = "${local.arn_prefix}:emr-serverless:${var.aws_region}:${local.account_id}:/applications/*" }
      }
    }]
  })
}
resource "aws_iam_role_policy" "emr_job" {
  role = aws_iam_role.emr_job.id
  policy = jsonencode({
    Version = "2012-10-17", Statement = [
      { Effect = "Allow", Action = ["s3:GetBucketLocation"], Resource = aws_s3_bucket.lake.arn },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.lake.arn,
      Condition = { StringLike = { "s3:prefix" = ["raw/*", "bronze/*", "silver/*", "gold/*", "artifacts/*", "manifests/*", "quality/*", "emr-logs/*"] } } },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = [for prefix in ["raw", "bronze", "silver", "gold", "artifacts", "manifests", "quality"] : "${aws_s3_bucket.lake.arn}/${prefix}/*"] },
      { Effect = "Allow", Action = ["s3:PutObject", "s3:AbortMultipartUpload"], Resource = [for prefix in ["bronze", "silver", "gold", "manifests", "quality", "emr-logs"] : "${aws_s3_bucket.lake.arn}/${prefix}/*"] },
      # Hadoop/Spark committers may remove task-temporary objects. Raw and final
      # curated objects have no deletion grant in this runtime role.
      { Effect = "Allow", Action = ["s3:DeleteObject"], Resource = [for prefix in ["bronze", "silver", "gold"] : "${aws_s3_bucket.lake.arn}/${prefix}/*/_temporary/*"] },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.platform.arn },
      { Effect = "Allow", Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition", "glue:CreateTable", "glue:UpdateTable", "glue:BatchCreatePartition"], Resource = [
        "${local.arn_prefix}:glue:${var.aws_region}:${local.account_id}:catalog",
        aws_glue_catalog_database.commerce.arn,
        "${local.arn_prefix}:glue:${var.aws_region}:${local.account_id}:table/${aws_glue_catalog_database.commerce.name}/*"
      ] },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"], Resource = [aws_cloudwatch_log_group.emr.arn, "${aws_cloudwatch_log_group.emr.arn}:log-stream:*"] }
    ]
  })
}
resource "aws_emrserverless_application" "spark" {
  name          = "${local.name}-spark"
  release_label = var.emr_release_label
  type          = "spark"
  architecture  = "X86_64"
  auto_start_configuration { enabled = true }
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 5
  }
  maximum_capacity {
    cpu    = "8 vCPU"
    memory = "32 GB"
  }
  # No pre-initialized workers. Built-in same-region service access avoids NAT.
  monitoring_configuration {
    cloudwatch_logging_configuration {
      enabled        = true
      log_group_name = aws_cloudwatch_log_group.emr.name
    }
    s3_monitoring_configuration { log_uri = "s3://${aws_s3_bucket.lake.id}/emr-logs/" }
  }
}
