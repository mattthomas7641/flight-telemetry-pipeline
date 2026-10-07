# --- Pipeline role (IRSA): writer + compactor service account -----------------------

data "aws_iam_policy_document" "pipeline_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [var.eks_oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider_url}:sub"
      values   = ["system:serviceaccount:${var.namespace}:flightline"]
    }
    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "flightline-${var.env}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.pipeline_trust.json
}

data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "WriteAndCompactLake"
    actions   = ["s3:PutObject", "s3:PutObjectTagging", "s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/telemetry/*"]
  }
  statement {
    sid       = "ListLake"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["telemetry/*"]
    }
  }
  statement {
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = [aws_kms_key.lake.arn]
  }
}

resource "aws_iam_role_policy" "pipeline" {
  role   = aws_iam_role.pipeline.id
  policy = data.aws_iam_policy_document.pipeline.json
}

# --- Analyst access: classification-scoped, attribute-based ---------------------------
# Engineers read internal and proprietary data freely. Export-controlled data and the
# quarantine (unvetted, so treated as export controlled) additionally require the
# principal to carry export_cleared=true, typically set by the IdP from HR records.

data "aws_iam_policy_document" "analyst" {
  statement {
    sid     = "ReadUnrestrictedClasses"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/telemetry/classification=internal/*",
      "${aws_s3_bucket.lake.arn}/telemetry/classification=proprietary/*",
    ]
  }
  statement {
    sid     = "ReadExportControlledIfCleared"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/telemetry/classification=export_controlled/*",
      "${aws_s3_bucket.lake.arn}/telemetry/_quarantine/*",
    ]
    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalTag/export_cleared"
      values   = ["true"]
    }
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.lake.arn]
  }
}

resource "aws_iam_policy" "analyst" {
  name   = "flightline-${var.env}-analyst-read"
  policy = data.aws_iam_policy_document.analyst.json
}
