output "github_actions_role_arn" {
  description = "IAM role granted EKS access."
  value       = var.github_actions_role_arn
}

output "eks_cluster_name" {
  description = "EKS cluster receiving the access entry."
  value       = var.eks_cluster_name
}

output "kubernetes_namespace" {
  description = "Namespace scope for the EKS access policy association."
  value       = var.kubernetes_namespace
}

output "eks_access_policy_arn" {
  description = "EKS access policy associated with the role."
  value       = local.eks_access_policy_arn
}
