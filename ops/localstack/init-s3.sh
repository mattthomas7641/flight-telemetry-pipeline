#!/bin/sh
# Runs when LocalStack is ready: create the lake bucket with the same baseline
# controls the Terraform module applies in AWS.
set -eu
awslocal s3api create-bucket --bucket flightline-telemetry
awslocal s3api put-bucket-versioning --bucket flightline-telemetry \
  --versioning-configuration Status=Enabled
awslocal s3api put-public-access-block --bucket flightline-telemetry \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
