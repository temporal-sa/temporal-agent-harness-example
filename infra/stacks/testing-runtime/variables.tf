variable "aws_region" {
  description = "AWS region for the runtime resources."
  type        = string
  default     = "us-west-1"
}

variable "eks_oidc_provider_arn" {
  description = "IAM OIDC provider ARN for the EKS cluster."
  type        = string
  default     = "arn:aws:iam::429214323166:oidc-provider/oidc.eks.us-west-1.amazonaws.com/id/A19A2DDF9700D5C8E559A966B862CDAD"
}

variable "eks_oidc_provider_host" {
  description = "OIDC provider host/path for the EKS cluster, without https://."
  type        = string
  default     = "oidc.eks.us-west-1.amazonaws.com/id/A19A2DDF9700D5C8E559A966B862CDAD"
}

variable "kubernetes_namespace" {
  description = "Kubernetes namespace for the testing app rollout."
  type        = string
  default     = "temporal-agent-harness"
}

variable "app_service_account_name" {
  description = "Service account used by the testing API deployment."
  type        = string
  default     = "agent-harness"
}

variable "worker_service_account_name" {
  description = "Service account used by the testing worker deployment."
  type        = string
  default     = "agent-harness-worker-testing"
}

variable "claimcheck_bucket_name" {
  description = "S3 bucket used for claim-check and artifact storage."
  type        = string
  default     = "michaelj-agent-harness-claimcheck-429214323166"
}

variable "oauth_table_name" {
  description = "DynamoDB table used for OAuth/session state."
  type        = string
  default     = "temporal-michaelj-agent-harness-demo-oauth"
}

variable "artifacts_table_name" {
  description = "DynamoDB table used for artifact metadata."
  type        = string
  default     = "temporal-michaelj-agent-harness-demo-artifacts"
}

variable "python_sandbox_lambda_function_name" {
  description = "Existing Lambda function invoked by the testing worker."
  type        = string
  default     = "temporal-michaelj-agent-harness-demo-python-sandbox-testing"
}

variable "app_role_name" {
  description = "IAM role name for the testing API service account."
  type        = string
  default     = "temporal-agent-harness-testing-app"
}

variable "worker_role_name" {
  description = "IAM role name for the testing worker service account."
  type        = string
  default     = "temporal-agent-harness-testing-worker"
}

variable "tags" {
  description = "Additional tags for runtime IAM resources."
  type        = map(string)
  default     = {}
}
