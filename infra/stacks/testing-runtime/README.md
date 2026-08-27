# Testing Runtime Stack

Manual Terraform stack for runtime IAM needed by the testing deployment in the
`temporal-agent-harness` Kubernetes namespace.

It creates only the IRSA roles used by the testing API and worker service
accounts. The existing S3 bucket, DynamoDB tables, and Python sandbox Lambda are
referenced by name and are not created here.

Run through the manual `Infra Testing Runtime` workflow, or locally:

```sh
terraform init
terraform plan
terraform apply
```

The GitHub Actions bootstrap role must first include the
`testing-runtime-iam-management` inline policy from `infra/bootstrap`.

The Kubernetes namespace must exist before deploying the app:

```sh
kubectl create namespace temporal-agent-harness \
  --dry-run=client \
  --output yaml \
  | kubectl apply -f -
```
