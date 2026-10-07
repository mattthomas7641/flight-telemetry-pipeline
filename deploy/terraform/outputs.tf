output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "pipeline_role_arn" {
  description = "Annotate the flightline ServiceAccount with this (overlays/aws)."
  value       = aws_iam_role.pipeline.arn
}

output "kms_key_arn" {
  value = aws_kms_key.lake.arn
}

output "analyst_policy_arn" {
  value = aws_iam_policy.analyst.arn
}
