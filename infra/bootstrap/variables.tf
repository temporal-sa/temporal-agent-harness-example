variable "aws_region" {
  description = "AWS region used by this demo account."
  type        = string
  default     = "us-west-1"
}

variable "github_owner" {
  description = "GitHub organization or user that owns the repository."
  type        = string
  default     = "temporal-sa"
}

variable "github_repository" {
  description = "GitHub repository name."
  type        = string
  default     = "temporal-agent-harness-example"
}

variable "github_environments" {
  description = "GitHub environment names allowed to assume the role."
  type        = set(string)
  default     = ["testing", "production", "infra"]
}

variable "additional_github_oidc_subjects" {
  description = "Additional GitHub OIDC sub claims or StringLike patterns allowed to assume the role."
  type        = set(string)
  default     = []
}

variable "create_github_oidc_provider" {
  description = "Create the GitHub Actions IAM OIDC provider. Set false if this AWS account already has one."
  type        = bool
  default     = false
}

variable "github_actions_role_name" {
  description = "IAM role name GitHub Actions will assume via OIDC."
  type        = string
  default     = "temporal-agent-harness-example-github-actions"
}

variable "state_bucket_name" {
  description = "Optional explicit Terraform state bucket name. Defaults to a repo/account/region-derived name."
  type        = string
  default     = null
}

variable "state_key_prefix" {
  description = "S3 key prefix under which Terraform state and lock files may be written."
  type        = string
  default     = "temporal-agent-harness-example"
}

variable "force_destroy_state_bucket" {
  description = "Allow Terraform to destroy a non-empty state bucket. Keep false outside throwaway accounts."
  type        = bool
  default     = false
}

variable "state_noncurrent_version_expiration_days" {
  description = "Days to retain old noncurrent Terraform state object versions."
  type        = number
  default     = 100
}

variable "enable_ecr_push_policy" {
  description = "Attach ECR push permissions for the app image repository."
  type        = bool
  default     = true
}

variable "ecr_repository_name" {
  description = "ECR repository GitHub Actions may push app images to."
  type        = string
  default     = "temporal-michaelj-agent-harness-demo"
}

variable "enable_eks_describe_policy" {
  description = "Attach eks:DescribeCluster for kubeconfig generation."
  type        = bool
  default     = true
}

variable "enable_eks_access_management_policy" {
  description = "Allow GitHub Actions to manage its own EKS access entry for the app namespace."
  type        = bool
  default     = true
}

variable "enable_testing_runtime_iam_management_policy" {
  description = "Allow GitHub Actions to manage testing runtime IAM roles with the configured prefix."
  type        = bool
  default     = true
}

variable "testing_runtime_role_name_prefix" {
  description = "IAM role name prefix GitHub Actions may manage for testing runtime IRSA roles."
  type        = string
  default     = "temporal-agent-harness-testing-"
}

variable "eks_cluster_name" {
  description = "EKS cluster GitHub Actions may describe."
  type        = string
  default     = "sa-demo"
}

variable "github_actions_eks_access_namespace" {
  description = "Kubernetes namespace GitHub Actions may grant itself EKS access to."
  type        = string
  default     = "temporal-agent-harness"
}

variable "eks_namespace_admin_policy_name" {
  description = "EKS access policy name that the GitHub Actions role may associate to itself in the app namespace."
  type        = string
  default     = "AmazonEKSAdminPolicy"
}

variable "tags" {
  description = "Additional tags for bootstrap-managed resources."
  type        = map(string)
  default     = {}
}
