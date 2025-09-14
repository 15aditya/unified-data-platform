# flink_job.py
import os
import sys
import json
from pyflink.table import EnvironmentSettings, TableEnvironment
from aws.aws_secrets import get_secret

# ---------- CONFIG / ENV ----------
# Secret name in Secrets Manager that contains Confluent credentials
SECRETS_NAME = os.environ.get("CONFLUENT_SECRETS_NAME", "confluent-cloud-secrets")
REGION = os.environ.get("AWS_REGION", "eu-central-1")

# These variables can be overridden via env vars (useful for local dev)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092")
SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "https://psrc-8vyvr.eu-central-1.aws.confluent.cloud")

# Iceberg / S3 config
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3a://my-prod-iceberg-warehouse/")
ICEBERG_CATALOG_NAME = os.environ.get("ICEBERG_CATALOG", "my_iceberg_catalog")
ICEBERG_DB = os.environ.get("ICEBERG_DB", "default")
ICEBERG_TABLE = os.environ.get("ICEBERG_TABLE", "clickouts_iceberg")

# ---------- Fetch secrets (once) ----------
secrets = get_secret(SECRETS_NAME, region_name=REGION)
KAFKA_API_KEY = secrets.get("KAFKA_API_KEY")
KAFKA_API_SECRET = secrets.get("KAFKA_API_SECRET")
SR_API_KEY = secrets.get("SCHEMA_REGISTRY_API_KEY") or secrets.get("SR_API_KEY")
SR_API_SECRET = secrets.get("SCHEMA_REGISTRY_API_SECRET") or secrets.get("SR_API_SECRET")

if not (KAFKA_API_KEY and KAFKA_API_SECRET and SR_API_KEY and SR_API_SECRET):
    raise RuntimeError("Missing Confluent credentials in Secrets Manager secret.")

# Build SASL JAAS config string
sasl_jaas = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="{k}" password="{p}";'
).format(k=KAFKA_API_KEY, p=KAFKA_API_SECRET)

# ---------- Create TableEnvironment ----------
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)

# (Optional) set parallelism from env
parallelism = int(os.environ.get("FLINK_PARALLELISM", "4"))
t_env.get_config().get_configuration().set_integer("parallelism.default", parallelism)

# ---------- Register Iceberg catalog ----------
# We use a Hive/Glue-backed catalog in production. For local dev, you can use 'hadoop' catalog pointing to MinIO.
# Example here uses 'iceberg' catalog with 'hive' or 'glue' catalog-type -- adjust for your environment.

# For EMR / Production prefer Glue catalog (no access keys in job; rely on instance role)
# For local dev with MinIO, set S3 config properties in Flink config or pass here via WITH options.

catalog_ddl = f"""
CREATE CATALOG IF NOT EXISTS {ICEBERG_CATALOG_NAME} WITH (
  'type'='iceberg',
  'catalog-type'='glue',                        -- use 'hive' or 'hadoop' for non-AWS setups
  'catalog-name' = 'prod_iceberg_catalog',
  'warehouse' = '{ICEBERG_WAREHOUSE}'
)
"""
t_env.execute_sql(catalog_ddl)

# Set current catalog and database
t_env.use_catalog(ICEBERG_CATALOG_NAME)
t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_DB}")
t_env.use_database(ICEBERG_DB)

# ---------- Create Kafka source table (clickouts) ----------
# Using avro-confluent format. We inject SR credentials via property values using placeholders.
kafka_source_ddl = f"""
CREATE TABLE kafka_clickouts (
  clickout_id STRING,
  user_id STRING,
  transaction_id STRING,
  amount DOUBLE,
  currency STRING,
  ts BIGINT,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'clickouts',
  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = '{sasl_jaas}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = '{SR_URL}',
  'avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
  'avro-confluent.basic-auth.user-info' = '{SR_API_KEY}:{SR_API_SECRET}'
)
"""
t_env.execute_sql(kafka_source_ddl)

# ---------- Create Iceberg sink table (if not exists) ----------
# Partitioning strategy: choose carefully. Example partitions by currency and bucket on clickout_id to avoid hotspotting.
iceberg_table_ddl = f"""
CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG_NAME}.{ICEBERG_DB}.{ICEBERG_TABLE} (
  clickout_id STRING,
  user_id STRING,
  transaction_id STRING,
  amount DOUBLE,
  currency STRING,
  ts BIGINT
) PARTITIONED BY (currency)
WITH (
  'write.format.default' = 'parquet'
)
"""
t_env.execute_sql(iceberg_table_ddl)

# ---------- Insert streaming data from Kafka to Iceberg ----------
insert_sql = f"""
INSERT INTO {ICEBERG_CATALOG_NAME}.{ICEBERG_DB}.{ICEBERG_TABLE}
SELECT clickout_id, user_id, transaction_id, amount, currency, ts FROM kafka_clickouts
"""
# execute_insert returns a TableResult which represents a running job for streaming inserts
print("Starting streaming insert job...")
t_env.execute_sql(insert_sql)
print("Streaming job submitted. Job is running (blocking in session mode may vary).")
