data "aws_partition" "current" {}

locals {
  eks_access_policy_arn = "arn:${data.aws_partition.current.partition}:eks::aws:cluster-access-policy/${var.eks_access_policy_name}"
}

resource "aws_eks_access_entry" "github_actions" {
  cluster_name  = var.eks_cluster_name
  principal_arn = var.github_actions_role_arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "github_actions_namespace_admin" {
  cluster_name  = var.eks_cluster_name
  principal_arn = aws_eks_access_entry.github_actions.principal_arn
  policy_arn    = local.eks_access_policy_arn

  access_scope {
    type       = "namespace"
    namespaces = [var.kubernetes_namespace]
  }
}
