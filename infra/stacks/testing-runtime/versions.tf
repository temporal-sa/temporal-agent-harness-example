terraform {
  required_version = ">= 1.15.0"

  backend "s3" {
    bucket       = "temporal-sa-terraform-state-429214323166-us-west-1"
    key          = "temporal-agent-harness-example/testing-runtime.tfstate"
    region       = "us-west-1"
    use_lockfile = true
    encrypt      = true
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
