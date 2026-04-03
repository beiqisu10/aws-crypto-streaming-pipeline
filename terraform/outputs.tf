# =================================================================
# TERRAFORM OUTPUTS (Infrastructure Metadata)
# =================================================================

# MSK Cluster connection string for Kafka Producers and Spark Consumers
output "msk_bootstrap_brokers" {
  description = "MSK Kafka connection string for bootstrap servers"
  value       = aws_msk_cluster.crypto_msk.bootstrap_brokers
}

# S3 Bucket ID used as the Data Lake landing zone and script repository
output "s3_bucket_name" {
  description = "The name of the S3 bucket acting as the Data Lake"
  value       = aws_s3_bucket.crypto_lake.id
}

# The public/private URL to access the Airflow Management UI
output "mwaa_webserver_url" {
  description = "The URL of the Apache Airflow Web UI"
  value       = "https://${aws_mwaa_environment.crypto_airflow.webserver_url}"
}

# MWAA Amazon Resource Name (ARN) for IAM policy debugging and cross-account access
output "mwaa_arn" {
  description = "The ARN of the MWAA environment"
  value       = aws_mwaa_environment.crypto_airflow.arn
}

# MWAA Security Group ID required for configuring Redshift Inbound/Ingress rules
output "mwaa_security_group_id" {
  description = "The ID of the security group associated with MWAA"
  value       = aws_security_group.mwaa_sg.id
}