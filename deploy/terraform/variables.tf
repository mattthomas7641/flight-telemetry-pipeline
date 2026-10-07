variable "region" {
  type    = string
  default = "us-east-1"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "bucket_name" {
  type        = string
  description = "Telemetry data lake bucket. Must be globally unique."
}

variable "eks_oidc_provider_arn" {
  type        = string
  description = "IAM OIDC provider ARN of the EKS cluster (for IRSA)."
}

variable "eks_oidc_provider_url" {
  type        = string
  description = "OIDC issuer URL without https://, e.g. oidc.eks.us-east-1.amazonaws.com/id/ABC."
}

variable "namespace" {
  type    = string
  default = "flightline"
}

# Must match config/catalog.yaml retention_classes; lifecycle rules are keyed off the
# retention_class object tag the writer sets on every object.
variable "retention_days" {
  type = map(number)
  default = {
    ephemeral   = 30
    engineering = 365
    flight_test = 2555
  }
}
