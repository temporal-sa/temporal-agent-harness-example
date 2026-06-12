# Managed Infrastructure Stacks

Put non-bootstrap Terraform stacks here.

These stacks should use the S3 backend created by `../bootstrap` and should be
runnable from manually dispatched GitHub Actions after the GitHub OIDC role is
available.

Good candidates for this area:

- namespace-scoped EKS access entries or Kubernetes RBAC for deployment;
- worker-controller installation and its namespace-scoped configuration;
- Python sandbox Lambda and its execution/invoke roles;
- app-owned AWS resources that are not changed on every deploy.

Keep routine app rollout concerns out of these stacks unless the resource is
infrastructure rather than a per-commit deployment artifact.

## Stacks

| Stack | Purpose |
| --- | --- |
| `eks-access` | Grants the GitHub Actions OIDC role namespace-scoped EKS access for `temporal-agent-harness`. |
| `testing-runtime` | Creates the testing app and worker IRSA roles trusted by service accounts in `temporal-agent-harness`. |
