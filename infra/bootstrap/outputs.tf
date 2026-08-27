output "github_actions_role_arn" {
  description = "IAM role ARN for GitHub Actions OIDC."
  value       = aws_iam_role.github_actions.arn
}

output "github_oidc_provider_arn" {
  description = "IAM OIDC provider ARN trusted by the GitHub Actions role."
  value       = local.github_oidc_provider_arn
}

output "github_oidc_subjects" {
  description = "GitHub OIDC sub claims allowed to assume the role."
  value       = local.github_oidc_subjects
}

output "terraform_state_bucket_name" {
  description = "S3 bucket name for Terraform remote state."
  value       = aws_s3_bucket.terraform_state.bucket
}

output "terraform_state_key_prefix" {
  description = "Allowed S3 key prefix for Terraform state and lock files."
  value       = var.state_key_prefix
}

output "terraform_backend_hcl" {
  description = "Backend config snippet for future Terraform stacks."
  value       = <<-EOT
    bucket       = "${aws_s3_bucket.terraform_state.bucket}"
    key          = "${var.state_key_prefix}/<stack-name>.tfstate"
    region       = "${var.aws_region}"
    encrypt      = true
    use_lockfile = true
  EOT
}

output "github_actions_eks_access_namespace" {
  description = "Kubernetes namespace the GitHub Actions role may grant itself EKS access to."
  value       = var.github_actions_eks_access_namespace
}
