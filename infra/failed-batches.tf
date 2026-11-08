# Lambda validates bucket-wide PutObject access when configuring an S3 failure
# destination. Keep that grant isolated from authoritative lake prefixes.
resource "aws_s3_bucket" "failed_batches" {
  bucket        = "${local.name}-failed-${local.account_id}-${var.aws_region}"
  force_destroy = false
}
resource "aws_s3_bucket_public_access_block" "failed_batches" {
  bucket                  = aws_s3_bucket.failed_batches.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_ownership_controls" "failed_batches" {
  bucket = aws_s3_bucket.failed_batches.id
  rule { object_ownership = "BucketOwnerEnforced" }
}
resource "aws_s3_bucket_versioning" "failed_batches" {
  bucket = aws_s3_bucket.failed_batches.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "failed_batches" {
  bucket = aws_s3_bucket.failed_batches.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.platform.arn
    }
    bucket_key_enabled = true
  }
}
resource "aws_s3_bucket_policy" "failed_batches" {
  bucket = aws_s3_bucket.failed_batches.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect    = "Deny", Principal = "*", Action = "s3:*"
    Resource  = [aws_s3_bucket.failed_batches.arn, "${aws_s3_bucket.failed_batches.arn}/*"]
    Condition = { Bool = { "aws:SecureTransport" = "false" } }
  }] })
}
resource "aws_s3_bucket_lifecycle_configuration" "failed_batches" {
  bucket     = aws_s3_bucket.failed_batches.id
  depends_on = [aws_s3_bucket_versioning.failed_batches]
  rule {
    id     = "failed-invocation-retention"
    status = "Enabled"
    filter {}
    expiration { days = 14 }
    noncurrent_version_expiration { noncurrent_days = 14 }
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}
