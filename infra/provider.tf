terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 1.66"
    }
  }

  required_version = ">= 1.6.0"
}

# --- AWS Provider ---
provider "aws" {
  region = var.aws_region
}

# --- Fetch Confluent Cloud API creds from AWS Secrets Manager ---
data "aws_secretsmanager_secret" "confluent_cloud" {
  name = "workshop/unified_data_platform/confluent_kafka_keys"
}

data "aws_secretsmanager_secret_version" "confluent_cloud" {
  secret_id = data.aws_secretsmanager_secret.confluent_cloud.id
}

locals {
  confluent_secrets = jsondecode(data.aws_secretsmanager_secret_version.confluent_cloud.secret_string)
}

# --- Confluent Cloud Provider ---
provider "confluent" {
  cloud_api_key    = local.confluent_secrets["confluent_cloud_api_key"]
  cloud_api_secret = local.confluent_secrets["confluent_cloud_api_secret"]
}
