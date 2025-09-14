# # Fetch current Confluent environment: Just to test connectivity
# data "confluent_environment" "default" {
#   id = "env-gxvzdr"
# }
#
# output "env_display_name" {
#   value = data.confluent_environment.default.display_name
# }

# Define Kafka topics
resource "confluent_kafka_topic" "transactions" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }

  topic_name       = "transactions"
  partitions_count = 6

  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = "2592000000" # 30 days in ms
    "min.insync.replicas" = "2"
  }

  credentials {
    key    = local.confluent_secrets["kafka_cluster_api_key"]
    secret = local.confluent_secrets["kafka_cluster_api_secret"]
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_topic" "users" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }

  topic_name       = "users"
  partitions_count = 6

  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = "2592000000" # 30 days in ms
    "min.insync.replicas" = "2"
  }

  credentials {
    key    = local.confluent_secrets["kafka_cluster_api_key"]
    secret = local.confluent_secrets["kafka_cluster_api_secret"]
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_topic" "hits" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }

  topic_name       = "hits"
  partitions_count = 12

  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = "2592000000" # 30 days in ms
    "min.insync.replicas" = "2"
  }

  credentials {
    key    = local.confluent_secrets["kafka_cluster_api_key"]
    secret = local.confluent_secrets["kafka_cluster_api_secret"]
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_topic" "clickout" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }

  topic_name       = "clickout"
  partitions_count = 6

  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = "2592000000" # 30 days in ms
    "min.insync.replicas" = "2"
  }

  credentials {
    key    = local.confluent_secrets["kafka_cluster_api_key"]
    secret = local.confluent_secrets["kafka_cluster_api_secret"]
  }

  lifecycle {
    prevent_destroy = true
  }
}
