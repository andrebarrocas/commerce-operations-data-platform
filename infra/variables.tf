variable "project_name" {
  type    = string
  default = "commerce-platform"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,24}$", var.project_name))
    error_message = "Use a 3..25 character lowercase resource prefix."
  }
}
variable "environment" {
  type    = string
  default = "dev"
  validation {
    condition     = contains(["dev", "test"], var.environment)
    error_message = "This portfolio deployment supports isolated dev/test environments."
  }
}
variable "aws_region" {
  type    = string
  default = "eu-west-1"
}
variable "lambda_zip_path" {
  type        = string
  default     = "../dist/lambda.zip"
  description = "Real artifact produced by scripts/package_lambda.py before plan."
}
variable "raw_retention_days" {
  type    = number
  default = 90
  validation {
    condition     = var.raw_retention_days >= 7 && var.raw_retention_days <= 365
    error_message = "Keep replay retention between 7 and 365 days and document its cost."
  }
}
variable "alarm_action_arns" {
  type        = list(string)
  default     = []
  description = "Existing notification actions. Empty creates visible alarms without notifications."
}
variable "emr_release_label" {
  type    = string
  default = "emr-7.12.0"
}
variable "enable_redshift" {
  type    = bool
  default = false
}
variable "redshift_subnet_ids" {
  type        = list(string)
  default     = []
  description = "Existing private subnets across the AZs required in the selected region."
}
variable "redshift_security_group_ids" {
  type        = list(string)
  default     = []
  description = "Existing groups permitting only intended private clients. No public ingress."
}
