resource "aws_iam_role" "redshift" {
  count = var.enable_redshift ? 1 : 0
  name  = "${local.name}-redshift"
  assume_role_policy = jsonencode({
    Version = "2012-10-17", Statement = [{
      Effect = "Allow", Principal = { Service = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"] }, Action = "sts:AssumeRole"
    }]
  })
}
resource "aws_iam_role_policy" "redshift" {
  count = var.enable_redshift ? 1 : 0
  role  = aws_iam_role.redshift[0].id
  policy = jsonencode({
    Version = "2012-10-17", Statement = [
      { Effect = "Allow", Action = ["s3:GetBucketLocation"], Resource = aws_s3_bucket.lake.arn },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.lake.arn, Condition = { StringLike = { "s3:prefix" = ["gold/*", "manifests/*"] } } },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${aws_s3_bucket.lake.arn}/gold/*", "${aws_s3_bucket.lake.arn}/manifests/*"] },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = aws_kms_key.platform.arn }
    ]
  })
}
resource "aws_redshiftserverless_namespace" "commerce" {
  count                 = var.enable_redshift ? 1 : 0
  namespace_name        = local.name
  db_name               = "commerce"
  admin_username        = "commerce_admin"
  manage_admin_password = true
  kms_key_id            = aws_kms_key.platform.arn
  iam_roles             = [aws_iam_role.redshift[0].arn]
  default_iam_role_arn  = aws_iam_role.redshift[0].arn
  log_exports           = ["userlog", "connectionlog", "useractivitylog"]
}
resource "aws_redshiftserverless_workgroup" "commerce" {
  count                = var.enable_redshift ? 1 : 0
  workgroup_name       = local.name
  namespace_name       = aws_redshiftserverless_namespace.commerce[0].namespace_name
  base_capacity        = 8
  max_capacity         = 8
  publicly_accessible  = false
  enhanced_vpc_routing = true
  subnet_ids           = var.redshift_subnet_ids
  security_group_ids   = var.redshift_security_group_ids
  config_parameter {
    parameter_key   = "require_ssl"
    parameter_value = "true"
  }
  lifecycle {
    precondition {
      condition     = length(var.redshift_subnet_ids) >= 3 && length(var.redshift_security_group_ids) > 0
      error_message = "Supply private subnets in at least three AZs and restricted security groups before enabling Redshift."
    }
  }
}
resource "aws_redshiftserverless_usage_limit" "daily" {
  count         = var.enable_redshift ? 1 : 0
  resource_arn  = aws_redshiftserverless_workgroup.commerce[0].arn
  usage_type    = "serverless-compute"
  amount        = 8
  period        = "daily"
  breach_action = "deactivate"
}
