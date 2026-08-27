data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  github_repository_full_name = "${var.github_owner}/${var.github_repository}"
  github_oidc_provider_url    = "https://token.actions.githubusercontent.com"
  github_oidc_provider_host   = "token.actions.githubusercontent.com"

  state_bucket_name = coalesce(
    var.state_bucket_name,
    "temporal-sa-terraform-state-${data.aws_caller_identity.current.account_id}-${var.aws_region}",
  )

  github_environment_subjects = [
    for environment in var.github_environments :
    "repo:${local.github_repository_full_name}:environment:${environment}"
  ]

  github_oidc_subjects = sort(distinct(concat(
    local.github_environment_subjects,
    tolist(var.additional_github_oidc_subjects),
  )))

  existing_github_oidc_provider_arn = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/${local.github_oidc_provider_host}"
  ecr_repository_arn                = "arn:${data.aws_partition.current.partition}:ecr:${var.aws_region}:${data.aws_caller_identity.current.account_id}:repository/${var.ecr_repository_name}"
  eks_cluster_arn                   = "arn:${data.aws_partition.current.partition}:eks:${var.aws_region}:${data.aws_caller_identity.current.account_id}:cluster/${var.eks_cluster_name}"
  github_actions_access_entry_arn   = "arn:${data.aws_partition.current.partition}:eks:${var.aws_region}:${data.aws_caller_identity.current.account_id}:access-entry/${var.eks_cluster_name}/role/${data.aws_caller_identity.current.account_id}/${var.github_actions_role_name}/*"
  eks_namespace_admin_policy_arn    = "arn:${data.aws_partition.current.partition}:eks::aws:cluster-access-policy/${var.eks_namespace_admin_policy_name}"
  testing_runtime_role_arn_pattern  = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${var.testing_runtime_role_name_prefix}*"

  common_tags = merge(
    {
      Project    = "temporal-agent-harness-example"
      Component  = "bootstrap"
      ManagedBy  = "terraform"
      Repository = local.github_repository_full_name
    },
    var.tags,
  )
}

resource "aws_s3_bucket" "terraform_state" {
  bucket        = local.state_bucket_name
  force_destroy = var.force_destroy_state_bucket
}

resource "aws_s3_bucket_public_access_block" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    id     = "ExpireNoncurrentTerraformStateVersions"
    status = "Enabled"

    filter {
      prefix = ""
    }

    noncurrent_version_expiration {
      noncurrent_days = var.state_noncurrent_version_expiration_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.terraform_state]
}

data "aws_iam_policy_document" "terraform_state_bucket" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.terraform_state.arn,
      "${aws_s3_bucket.terraform_state.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id
  policy = data.aws_iam_policy_document.terraform_state_bucket.json
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 1 : 0

  url = local.github_oidc_provider_url

  client_id_list = [
    "sts.amazonaws.com",
  ]
}

locals {
  github_oidc_provider_arn = (
    var.create_github_oidc_provider
    ? aws_iam_openid_connect_provider.github[0].arn
    : local.existing_github_oidc_provider_arn
  )
}

data "aws_iam_policy_document" "github_actions_assume_role" {
  statement {
    sid     = "AllowGitHubActionsOidc"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.github_oidc_provider_host}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "${local.github_oidc_provider_host}:sub"
      values   = local.github_oidc_subjects
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name                 = var.github_actions_role_name
  assume_role_policy   = data.aws_iam_policy_document.github_actions_assume_role.json
  description          = "OIDC role for GitHub Actions in ${local.github_repository_full_name}"
  max_session_duration = 3600
}

data "aws_iam_policy_document" "terraform_state_access" {
  statement {
    sid    = "ListTerraformStatePrefix"
    effect = "Allow"

    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.terraform_state.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        var.state_key_prefix,
        "${var.state_key_prefix}/*",
      ]
    }
  }

  statement {
    sid    = "ManageTerraformStateObjects"
    effect = "Allow"

    actions = [
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:PutObject",
    ]

    resources = [
      "${aws_s3_bucket.terraform_state.arn}/${var.state_key_prefix}/*",
    ]
  }
}

resource "aws_iam_role_policy" "terraform_state_access" {
  name   = "terraform-state-access"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.terraform_state_access.json
}

data "aws_iam_policy_document" "ecr_push" {
  statement {
    sid       = "GetEcrAuthorizationToken"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "PushAppImage"
    effect = "Allow"

    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:CompleteLayerUpload",
      "ecr:DescribeImages",
      "ecr:DescribeRepositories",
      "ecr:GetDownloadUrlForLayer",
      "ecr:InitiateLayerUpload",
      "ecr:ListImages",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
    ]

    resources = [local.ecr_repository_arn]
  }
}

resource "aws_iam_role_policy" "ecr_push" {
  count = var.enable_ecr_push_policy ? 1 : 0

  name   = "ecr-push-${var.ecr_repository_name}"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.ecr_push.json
}

data "aws_iam_policy_document" "eks_describe" {
  statement {
    sid       = "DescribeEksCluster"
    effect    = "Allow"
    actions   = ["eks:DescribeCluster"]
    resources = [local.eks_cluster_arn]
  }
}

resource "aws_iam_role_policy" "eks_describe" {
  count = var.enable_eks_describe_policy ? 1 : 0

  name   = "eks-describe-${var.eks_cluster_name}"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.eks_describe.json
}

data "aws_iam_policy_document" "eks_access_management" {
  statement {
    sid    = "CreateOwnAccessEntry"
    effect = "Allow"

    actions   = ["eks:CreateAccessEntry"]
    resources = [local.eks_cluster_arn]

    condition {
      test     = "StringEquals"
      variable = "eks:principalArn"
      values   = [aws_iam_role.github_actions.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "eks:accessEntryType"
      values   = ["STANDARD"]
    }
  }

  statement {
    sid    = "ListTargetClusterAccessEntries"
    effect = "Allow"

    actions   = ["eks:ListAccessEntries"]
    resources = [local.eks_cluster_arn]
  }

  statement {
    sid       = "ListAccessPolicies"
    effect    = "Allow"
    actions   = ["eks:ListAccessPolicies"]
    resources = ["*"]
  }

  statement {
    sid    = "ReadAndDeleteOwnAccessEntry"
    effect = "Allow"

    actions = [
      "eks:DeleteAccessEntry",
      "eks:DescribeAccessEntry",
      "eks:ListAssociatedAccessPolicies",
    ]

    resources = [local.github_actions_access_entry_arn]
  }

  statement {
    sid    = "ManageOwnNamespaceAccessPolicy"
    effect = "Allow"

    actions = [
      "eks:AssociateAccessPolicy",
      "eks:DisassociateAccessPolicy",
    ]

    resources = [local.github_actions_access_entry_arn]

    condition {
      test     = "StringEquals"
      variable = "eks:policyArn"
      values   = [local.eks_namespace_admin_policy_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "eks:accessScope"
      values   = ["namespace"]
    }

    condition {
      test     = "ForAllValues:StringEquals"
      variable = "eks:namespaces"
      values   = [var.github_actions_eks_access_namespace]
    }
  }
}

resource "aws_iam_role_policy" "eks_access_management" {
  count = var.enable_eks_access_management_policy ? 1 : 0

  name   = "eks-access-management-${var.eks_cluster_name}"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.eks_access_management.json
}

data "aws_iam_policy_document" "testing_runtime_iam_management" {
  statement {
    sid    = "CreateTestingRuntimeRoles"
    effect = "Allow"

    actions = [
      "iam:CreateRole",
      "iam:TagRole",
    ]

    resources = [local.testing_runtime_role_arn_pattern]
  }

  statement {
    sid    = "ManageTestingRuntimeRoles"
    effect = "Allow"

    actions = [
      "iam:DeleteRole",
      "iam:DeleteRolePolicy",
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole",
      "iam:ListRolePolicies",
      "iam:ListRoleTags",
      "iam:PutRolePolicy",
      "iam:UntagRole",
      "iam:UpdateAssumeRolePolicy",
      "iam:UpdateRole",
      "iam:UpdateRoleDescription",
    ]

    resources = [local.testing_runtime_role_arn_pattern]
  }
}

resource "aws_iam_role_policy" "testing_runtime_iam_management" {
  count = var.enable_testing_runtime_iam_management_policy ? 1 : 0

  name   = "testing-runtime-iam-management"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.testing_runtime_iam_management.json
}
