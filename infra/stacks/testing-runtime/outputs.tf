output "app_role_arn" {
  description = "IRSA role ARN for the testing API service account."
  value       = aws_iam_role.app.arn
}

output "worker_role_arn" {
  description = "IRSA role ARN for the testing worker service account."
  value       = aws_iam_role.worker.arn
}

output "kubernetes_namespace" {
  description = "Kubernetes namespace the roles trust."
  value       = var.kubernetes_namespace
}

output "app_service_account_name" {
  description = "Kubernetes service account trusted by the app role."
  value       = var.app_service_account_name
}

output "worker_service_account_name" {
  description = "Kubernetes service account trusted by the worker role."
  value       = var.worker_service_account_name
}
