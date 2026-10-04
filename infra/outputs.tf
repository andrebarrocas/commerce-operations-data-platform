output "lake_bucket" { value = aws_s3_bucket.lake.id }
output "stream_name" { value = aws_kinesis_stream.events.name }
output "producer_policy_arn" { value = aws_iam_policy.producer.arn }
output "glue_database" { value = aws_glue_catalog_database.commerce.name }
output "emr_application_id" { value = aws_emrserverless_application.spark.id }
output "emr_execution_role_arn" { value = aws_iam_role.emr_job.arn }
output "redshift_workgroup" { value = try(aws_redshiftserverless_workgroup.commerce[0].workgroup_name, null) }
