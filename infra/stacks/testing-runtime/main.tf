data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  claimcheck_bucket_arn = "arn:${data.aws_partition.current.partition}:s3:::${var.claimcheck_bucket_name}"
  lambda_function_arn   = "arn:${data.aws_partition.current.partition}:lambda:${var.aws_region}:${data.aws_caller_identity.current.account_id}:function:${var.python_sandbox_lambda_function_name}"

  dynamodb_table_arns = [
    "arn:${data.aws_partition.current.partition}:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.oauth_table_name}",
    "arn:${data.aws_partition.current.partition}:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.artifacts_table_name}",
  ]

  dynamodb_index_arns = [
    for table_arn in local.dynamodb_table_arns : "${table_arn}/index/*"
  ]

  common_tags = merge(
    {
      Project   = "temporal-agent-harness-example"
      Component = "testing-runtime"
      ManagedBy = "terraform"
    },
    var.tags,
  )
}

data "aws_iam_policy_document" "app_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.eks_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider_host}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider_host}:sub"
      values = [
        "system:serviceaccount:${var.kubernetes_namespace}:${var.app_service_account_name}",
      ]
    }
  }
}

data "aws_iam_policy_document" "worker_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.eks_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider_host}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider_host}:sub"
      values = [
        "system:serviceaccount:${var.kubernetes_namespace}:${var.worker_service_account_name}",
      ]
    }
  }
}

resource "aws_iam_role" "app" {
  name                 = var.app_role_name
  assume_role_policy   = data.aws_iam_policy_document.app_assume_role.json
  description          = "IRSA role for testing API in ${var.kubernetes_namespace}"
  max_session_duration = 3600
  tags                 = local.common_tags
}

resource "aws_iam_role" "worker" {
  name                 = var.worker_role_name
  assume_role_policy   = data.aws_iam_policy_document.worker_assume_role.json
  description          = "IRSA role for testing worker in ${var.kubernetes_namespace}"
  max_session_duration = 3600
  tags                 = local.common_tags
}

data "aws_iam_policy_document" "shared_runtime_access" {
  statement {
    sid    = "ListClaimcheckBucket"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]

    resources = [local.claimcheck_bucket_arn]
  }

  statement {
    sid    = "ManageClaimcheckObjects"
    effect = "Allow"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]

    resources = ["${local.claimcheck_bucket_arn}/*"]
  }

  statement {
    sid    = "UseAppDynamoDbTables"
    effect = "Allow"

    actions = [
      "dynamodb:DeleteItem",
      "dynamodb:DescribeTable",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
    ]

    resources = concat(local.dynamodb_table_arns, local.dynamodb_index_arns)
  }
}

resource "aws_iam_role_policy" "app_runtime_access" {
  name   = "runtime-access"
  role   = aws_iam_role.app.id
  policy = data.aws_iam_policy_document.shared_runtime_access.json
}

resource "aws_iam_role_policy" "worker_runtime_access" {
  name   = "runtime-access"
  role   = aws_iam_role.worker.id
  policy = data.aws_iam_policy_document.shared_runtime_access.json
}

data "aws_iam_policy_document" "worker_lambda_access" {
  statement {
    sid    = "InvokePythonSandbox"
    effect = "Allow"

    actions = ["lambda:InvokeFunction"]

    resources = [
      local.lambda_function_arn,
      "${local.lambda_function_arn}:*",
    ]
  }
}

resource "aws_iam_role_policy" "worker_lambda_access" {
  name   = "python-sandbox-lambda-access"
  role   = aws_iam_role.worker.id
  policy = data.aws_iam_policy_document.worker_lambda_access.json
}
