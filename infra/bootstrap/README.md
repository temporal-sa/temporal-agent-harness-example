# Bootstrap Infrastructure

This stack bootstraps a fresh AWS account for this repository.

It creates:

- an S3 bucket for Terraform remote state, with versioning, encryption, public
  access blocking, and HTTPS-only access;
- a GitHub Actions OIDC provider, unless you point it at an existing one;
- a tightly scoped GitHub Actions IAM role trusted only by configured GitHub
  environment subjects from this repository;
- minimal role policies for Terraform state access, ECR image push, and EKS
  cluster description;
- narrowly scoped permission for the GitHub Actions role to manage only its own
  EKS access entry, and only with `AmazonEKSAdminPolicy` scoped to the
  `temporal-agent-harness` namespace;
- narrowly scoped permission for the GitHub Actions role to manage only IAM
  roles whose names start with `temporal-agent-harness-testing-`.

It does not install cluster components, mutate Kubernetes resources, or create
application runtime resources. Those should be separate Terraform stacks after
this bootstrap has run.

`eks:DescribeCluster` is included only so GitHub Actions can generate a
kubeconfig for the target cluster. It does not authorize Kubernetes API access.
Namespace-scoped EKS access entries or Kubernetes RBAC should be managed in a
separate follow-up stack once the cluster rollout pattern is selected.

The prefixed IAM management permission is included so manually dispatched
Terraform stacks can manage the testing app and worker IRSA roles without
granting broad IAM administration to GitHub Actions.

## Run

Use AWS credentials with enough permissions to create IAM roles, IAM OIDC
providers, IAM role policies, and S3 buckets.

```bash
cd infra/bootstrap
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

This stack starts with local Terraform state. Do not delete the generated local
state unless you deliberately migrate this bootstrap stack to the S3 backend.
The future non-bootstrap stacks should use the S3 backend from the outputs.

## Existing GitHub OIDC Provider

AWS accounts can have only one IAM OIDC provider for
`https://token.actions.githubusercontent.com`. If one already exists, set:

```hcl
create_github_oidc_provider = false
```

The stack will look up the existing provider by URL and create only the role and
state bucket.

Alternatively, import the existing provider into this stack:

```bash
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
terraform import \
  aws_iam_openid_connect_provider.github[0] \
  "arn:aws:iam::${ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"
```

## After Apply

Store the role ARN in GitHub environment variables. The ARN is not a secret.

```bash
ROLE_ARN="$(terraform output -raw github_actions_role_arn)"
gh variable set AWS_ROLE_ARN --env testing --body "$ROLE_ARN"
gh variable set AWS_ROLE_ARN --env production --body "$ROLE_ARN"
```

When an `infra` GitHub environment exists for manually dispatched infrastructure
workflows, set the same variable there too.

Future Terraform stacks should use a backend like:

```hcl
terraform {
  backend "s3" {
    bucket       = "<terraform_state_bucket_name output>"
    key          = "temporal-agent-harness-example/<stack-name>.tfstate"
    region       = "us-west-1"
    encrypt      = true
    use_lockfile = true
  }
}
```
