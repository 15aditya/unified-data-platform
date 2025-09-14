variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-central-1"
}

variable "kafka_cluster_id" {
  description = "Id of the kafka cluster"
  type = string
  default = "lkc-v0ywqn"
}