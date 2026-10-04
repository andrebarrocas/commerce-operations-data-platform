locals {
  alarms = {
    stream_write_throttle = {
      namespace   = "AWS/Kinesis", metric = "WriteProvisionedThroughputExceeded", statistic = "Sum", threshold = 1
      dimensions  = { StreamName = aws_kinesis_stream.events.name }
      description = "Reduce producer rate or investigate partition skew; bounded producer retries may exhaust."
    }
    raw_delivery_age = {
      namespace   = "AWS/Firehose", metric = "DeliveryToS3.DataFreshness", statistic = "Maximum", threshold = 300
      dimensions  = { DeliveryStreamName = aws_kinesis_firehose_delivery_stream.raw.name }
      description = "Raw delivery is more than five minutes behind. Check S3/KMS permissions and delivery logs."
    }
    validator_errors = {
      namespace   = "AWS/Lambda", metric = "Errors", statistic = "Sum", threshold = 1
      dimensions  = { FunctionName = aws_lambda_function.validator.function_name }
      description = "Validator invocation failed. Inspect logs, iterator age and S3 failed-batch evidence."
    }
    validator_lag = {
      namespace   = "AWS/Lambda", metric = "IteratorAge", statistic = "Maximum", threshold = 300000
      dimensions  = { FunctionName = aws_lambda_function.validator.function_name }
      description = "Streaming signals exceed five minutes of lag; authoritative raw delivery is a separate path."
    }
    quarantine_write_failure = {
      namespace   = "CommercePlatform", metric = "failures", statistic = "Sum", threshold = 1
      dimensions  = { Environment = var.environment, Pipeline = "commerce", Stage = "stream_attempt" }
      description = "A record could not be quarantined durably; partial batch failure requests retry."
    }
    batch_quality_failure = {
      namespace   = "CommercePlatform", metric = "quality_failures", statistic = "Sum", threshold = 1
      dimensions  = { Environment = var.environment, Pipeline = "commerce", Stage = "batch" }
      description = "Blocking batch quality gate failed; retain the previous committed release."
    }
  }
}
resource "aws_cloudwatch_metric_alarm" "pipeline" {
  for_each            = local.alarms
  alarm_name          = "${local.name}-${each.key}"
  alarm_description   = each.value.description
  namespace           = each.value.namespace
  metric_name         = each.value.metric
  statistic           = each.value.statistic
  dimensions          = each.value.dimensions
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = each.value.threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_action_arns
}
