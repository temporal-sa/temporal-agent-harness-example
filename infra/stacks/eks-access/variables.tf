variable "aws_region" {
  description = "AWS region containing the EKS cluster."
  type        = string
  default     = "us-west-1"
}

variable "eks_cluster_name" {
  description = "EKS cluster to grant access to."
  type        = string
  default     = "sa-demo"
}

variable "github_actions_role_arn" {
  description = "IAM role ARN assumed by GitHub Actions via OIDC."
  type        = string
}

variable "kubernetes_namespace" {
  description = "Kubernetes namespace GitHub Actions may administer."
  type        = string
  default     = "temporal-agent-harness"
}

variable "eks_access_policy_name" {
  description = "AWS-managed EKS access policy to associate with the GitHub Actions role."
  type        = string
  default     = "AmazonEKSAdminPolicy"
}
