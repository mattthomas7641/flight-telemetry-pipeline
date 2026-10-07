resource "aws_kms_key" "lake" {
  description             = "flightline telemetry lake (${var.env})"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "lake" {
  name          = "alias/flightline-${var.env}-lake"
  target_key_id = aws_kms_key.lake.key_id
}
