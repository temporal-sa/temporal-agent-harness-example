# Infrastructure

This directory contains infrastructure-as-code for the deployed demo.

## Layout

| Path | Purpose |
| --- | --- |
| `bootstrap/` | Manual local bootstrap for a new AWS account. Creates the GitHub Actions OIDC trust role and Terraform state bucket. |
| `stacks/eks-access/` | Remote-state-backed stack granting the GitHub Actions role namespace-scoped EKS access. |
| `stacks/testing-runtime/` | Remote-state-backed stack creating the testing app and worker IRSA roles for `temporal-agent-harness`. |

The bootstrap stack is intentionally small and locally run because GitHub
Actions cannot assume an AWS role or use remote Terraform state until those
resources exist.

Future long-lived infrastructure that does not need to change on every deploy
should live in separate Terraform stacks under this directory and use the S3
backend created by `bootstrap/`. Those stacks can then be run by manually
dispatched GitHub Actions using the OIDC role.

Routine app deploys should stay narrower than the infra stacks: build/push the
image, update Kubernetes resources, and wait for rollout.

The target Kubernetes namespace for the new rollout path is
`temporal-agent-harness`. Keep production on the existing namespace until the
testing deployment path has been validated.

## Current Order

1. Run `bootstrap/` locally after changes to the GitHub role policies.
2. Run the manual `Infra EKS Access` workflow.
3. Run the manual `Infra Testing Runtime` workflow.
4. Run the manual `Deploy Testing` workflow.

The Kubernetes namespace itself must exist before the deploy workflow applies
namespaced resources.

## Temporary Rollout Gate

Until testing is validated, GitHub Actions added by this repo should be manual
or testing-only. Do not add automatic production triggers on `push`, PR merge,
or `main` until the testing path has been explicitly approved.
