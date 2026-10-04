resource "aws_kinesis_stream" "events" {
  name             = "${local.name}-events"
  shard_count      = 1
  retention_period = 24
  encryption_type  = "KMS"
  kms_key_id       = aws_kms_key.platform.arn
  shard_level_metrics = [
    "IncomingBytes", "IncomingRecords", "WriteProvisionedThroughputExceeded", "IteratorAgeMilliseconds"
  ]
  stream_mode_details { stream_mode = "PROVISIONED" }
}
resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.name}-raw"
  retention_in_days = 14
}
resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}
resource "aws_iam_role" "firehose" {
  name = "${local.name}-firehose"
  assume_role_policy = jsonencode({
    Version = "2012-10-17", Statement = [{
      Effect    = "Allow", Principal = { Service = "firehose.amazonaws.com" }, Action = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = local.account_id } }
    }]
  })
}
resource "aws_iam_role_policy" "firehose" {
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17", Statement = [
      { Effect = "Allow", Action = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"], Resource = aws_kinesis_stream.events.arn },
      { Effect = "Allow", Action = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"], Resource = aws_s3_bucket.lake.arn },
      { Effect = "Allow", Action = ["s3:AbortMultipartUpload", "s3:GetObject", "s3:PutObject"], Resource = ["${aws_s3_bucket.lake.arn}/raw/*", "${aws_s3_bucket.lake.arn}/firehose-errors/*"] },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.platform.arn },
      { Effect = "Allow", Action = ["logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.firehose.arn}:log-stream:*" }
    ]
  })
}
resource "aws_kinesis_firehose_delivery_stream" "raw" {
  name        = "${local.name}-raw"
  destination = "extended_s3"
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.events.arn
    role_arn           = aws_iam_role.firehose.arn
  }
  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.lake.arn
    buffering_interval  = 60
    buffering_size      = 5
    compression_format  = "GZIP"
    kms_key_arn         = aws_kms_key.platform.arn
    prefix              = "raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "firehose-errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    processing_configuration {
      enabled = true
      processors { type = "AppendDelimiterToRecord" }
    }
    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }
  # Kinesis already encrypts the source. Firehose SSE cannot be configured for
  # a delivery stream whose source is Kinesis Data Streams.
  depends_on = [aws_iam_role_policy.firehose, aws_s3_bucket_server_side_encryption_configuration.lake]
}
resource "aws_iam_policy" "producer" {
  name = "${local.name}-producer"
  policy = jsonencode({
    Version = "2012-10-17", Statement = [
      { Effect = "Allow", Action = ["kinesis:PutRecords"], Resource = aws_kinesis_stream.events.arn },
      { Effect = "Allow", Action = ["kms:GenerateDataKey"], Resource = aws_kms_key.platform.arn }
    ]
  })
}
