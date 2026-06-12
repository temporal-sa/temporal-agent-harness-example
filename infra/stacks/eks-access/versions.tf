terraform {
  required_version = ">= 1.10.0"

  backend "s3" {
    bucket       = "temporal-sa-terraform-state-429214323166-us-west-1"
    key          = "temporal-agent-harness-example/eks-access.tfstate"
    region       = "us-west-1"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.50.0, < 7.0.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}
