resource "aws_cloudwatch_log_group" "validator" {
  name              = "/aws/lambda/${local.name}-validator"
  retention_in_days = 14
}
resource "aws_iam_role" "validator" {
  name = "${local.name}-validator"
  assume_role_policy = jsonencode({
    Version = "2012-10-17", Statement = [{
      Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole"
    }]
  })
}
resource "aws_iam_role_policy" "validator" {
  role = aws_iam_role.validator.id
  policy = jsonencode({
    Version = "2012-10-17", Statement = [
      { Effect = "Allow", Action = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"], Resource = aws_kinesis_stream.events.arn },
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = ["${aws_s3_bucket.lake.arn}/quarantine/stream/*", "${aws_s3_bucket.lake.arn}/aws/lambda/*"], Condition = { StringEquals = { "s3:ResourceAccount" = local.account_id } } },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.lake.arn, Condition = { StringEquals = { "s3:ResourceAccount" = local.account_id } } },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.platform.arn },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.validator.arn}:log-stream:*" }
    ]
  })
}
resource "aws_lambda_function" "validator" {
  function_name                  = "${local.name}-validator"
  role                           = aws_iam_role.validator.arn
  filename                       = var.lambda_zip_path
  source_code_hash               = filebase64sha256(var.lambda_zip_path)
  handler                        = "lambda_handler.handler"
  runtime                        = "python3.11"
  architectures                  = ["x86_64"]
  memory_size                    = 256
  timeout                        = 30
  reserved_concurrent_executions = 2
  environment {
    variables = {
      LAKE_BUCKET = aws_s3_bucket.lake.id
      ENVIRONMENT = var.environment
    }
  }
  depends_on = [aws_cloudwatch_log_group.validator, aws_iam_role_policy.validator]
}
resource "aws_lambda_event_source_mapping" "validator" {
  event_source_arn                   = aws_kinesis_stream.events.arn
  function_name                      = aws_lambda_function.validator.arn
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = 100
  maximum_batching_window_in_seconds = 1
  maximum_record_age_in_seconds      = 3600
  maximum_retry_attempts             = 3
  bisect_batch_on_function_error     = true
  function_response_types            = ["ReportBatchItemFailures"]
  parallelization_factor             = 1
  destination_config {
    on_failure { destination_arn = aws_s3_bucket.lake.arn }
  }
  depends_on = [aws_iam_role_policy.validator, aws_s3_bucket_server_side_encryption_configuration.lake]
}
