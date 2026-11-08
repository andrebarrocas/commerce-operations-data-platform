# Temporary isolated network for supervised acceptance. No public IPs or NAT.
# This fixture is deliberately separate from the reusable platform module.
terraform {
  required_version = ">= 1.10.0, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.67.0"
    }
  }
}
variable "expected_account" { type = string }
variable "project_name" { type = string }
variable "lambda_zip_path" { type = string }
variable "aws_region" {
  type    = string
  default = "eu-west-1"
}
provider "aws" {
  region              = var.aws_region
  allowed_account_ids = [var.expected_account]
  default_tags {
    tags = { Project = var.project_name, Environment = "test", Purpose = "temporary-acceptance" }
  }
}
data "aws_availability_zones" "available" { state = "available" }
resource "aws_vpc" "test" {
  cidr_block           = "10.83.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
}
resource "aws_subnet" "test" {
  count                   = 3
  vpc_id                  = aws_vpc.test.id
  cidr_block              = cidrsubnet(aws_vpc.test.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = false
}
resource "aws_route_table" "test" { vpc_id = aws_vpc.test.id }
resource "aws_route_table_association" "test" {
  count          = 3
  subnet_id      = aws_subnet.test[count.index].id
  route_table_id = aws_route_table.test.id
}
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.test.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.test.id]
  policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect   = "Allow", Principal = "*", Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
    Resource = ["arn:aws:s3:::${var.project_name}-test-${var.expected_account}-${var.aws_region}", "arn:aws:s3:::${var.project_name}-test-${var.expected_account}-${var.aws_region}/*"]
  }] })
}
resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-acceptance"
  description = "No inbound access; Data API only. HTTPS egress to S3 gateway."
  vpc_id      = aws_vpc.test.id
  egress {
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    prefix_list_ids = [aws_vpc_endpoint.s3.prefix_list_id]
  }
}
module "platform" {
  source                         = "../"
  project_name                   = var.project_name
  environment                    = "test"
  aws_region                     = var.aws_region
  lambda_zip_path                = var.lambda_zip_path
  validator_reserved_concurrency = -1
  enable_redshift                = true
  redshift_subnet_ids            = aws_subnet.test[*].id
  redshift_security_group_ids    = [aws_security_group.redshift.id]
}
output "platform" {
  value = {
    lake_bucket            = module.platform.lake_bucket
    failed_batches_bucket  = module.platform.failed_batches_bucket
    stream_name            = module.platform.stream_name
    emr_application_id     = module.platform.emr_application_id
    emr_execution_role_arn = module.platform.emr_execution_role_arn
    redshift_workgroup     = module.platform.redshift_workgroup
    glue_database          = module.platform.glue_database
  }
}
