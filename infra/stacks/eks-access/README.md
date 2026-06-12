# EKS Access

This stack grants the GitHub Actions OIDC role Kubernetes access to the app
namespace through EKS access entries.

It creates:

- one EKS access entry for the GitHub Actions IAM role;
- one namespace-scoped `AmazonEKSAdminPolicy` association for
  `temporal-agent-harness`.

This stack does not create the Kubernetes namespace. EKS accepts namespace names
in access policy associations without verifying that the namespace exists. The
namespace and route cutover should be handled by the deployment workflow for the
testing environment first, then production after validation.

## Run From GitHub

Use the `Infra EKS Access` workflow:

- `apply`: `false` for plan-only, `true` to apply

The workflow runs in the `infra` GitHub environment and assumes `AWS_ROLE_ARN`.

## Bootstrap Prerequisite

After pulling this change, re-run the local bootstrap stack once so the GitHub
role receives permission to manage its own EKS access entry:

```bash
cd infra/bootstrap
terraform plan
terraform apply
```

Then run this stack from GitHub.

## Local Run

If needed, run locally with an AWS identity that can use the remote state bucket
and manage access entries on the target cluster:

```bash
cd infra/stacks/eks-access
terraform init
terraform plan -var 'github_actions_role_arn=arn:aws:iam::429214323166:role/temporal-agent-harness-example-github-actions'
terraform apply -var 'github_actions_role_arn=arn:aws:iam::429214323166:role/temporal-agent-harness-example-github-actions'
```
