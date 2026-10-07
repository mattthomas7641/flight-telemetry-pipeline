resource "aws_s3_bucket" "lake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule { object_ownership = "BucketOwnerEnforced" } # ACLs off; IAM is the only access path
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true # cuts KMS request cost on high object counts
  }
}

# Retention is enforced by S3, not by a cleanup script: each object carries a
# retention_class tag (set atomically at PUT by the writer) and expires by rule.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  dynamic "rule" {
    for_each = var.retention_days
    content {
      id     = "retention-${rule.key}"
      status = "Enabled"
      filter {
        tag {
          key   = "retention_class"
          value = rule.key
        }
      }
      dynamic "transition" {
        for_each = rule.value > 180 ? [1] : []
        content {
          days          = 90
          storage_class = "GLACIER_IR" # flight-test data is rarely read after a campaign
        }
      }
      expiration { days = rule.value }
      noncurrent_version_expiration { noncurrent_days = 30 }
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 2 }
  }
}

data "aws_iam_policy_document" "lake_bucket" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # Governance invariant enforced server-side: an object without a classification tag
  # cannot be written, even by a buggy or compromised writer.
  statement {
    sid       = "DenyUntaggedWrites"
    effect    = "Deny"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Null"
      variable = "s3:RequestObjectTag/classification"
      values   = ["true"]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = data.aws_iam_policy_document.lake_bucket.json
}
