# Fetch current Confluent environment: Just to test connectivity
data "confluent_environment" "default" {
  id = "env-gxvzdr"
}

output "env_display_name" {
  value = data.confluent_environment.default.display_name
}
