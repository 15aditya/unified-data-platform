import time
import uuid
import random
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from aws.aws_secrets import get_secret


# 🔑 Fetch secrets from AWS Secrets Manager
secrets = get_secret("confluent-cloud-secrets")  # update to your secret name

# Kafka topics
TOPIC_HITS = "hits"
TOPIC_CLICKOUTS = "clickouts"

# Broker & Schema Registry
KAFKA_BROKER = "pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092"
SCHEMA_REGISTRY_URL = "https://psrc-8vyvr.eu-central-1.aws.confluent.cloud"

# Avro Schemas
hit_schema_str = """
{
  "namespace": "com.example",
  "name": "Hit",
  "type": "record",
  "fields": [
    {"name": "hit_id", "type": "string"},
    {"name": "clickout_id", "type": ["null", "string"], "default": null},
    {"name": "user_id", "type": "string"},
    {"name": "session_id", "type": "string"},
    {"name": "page", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

clickout_schema_str = """
{
  "namespace": "com.example",
  "name": "Clickout",
  "type": "record",
  "fields": [
    {"name": "clickout_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "transaction_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

# Schema Registry
schema_registry_conf = {
    "url": SCHEMA_REGISTRY_URL,
    "basic.auth.user.info": f"{secrets['SCHEMA_REGISTRY_API_KEY']}:{secrets['SCHEMA_REGISTRY_API_SECRET']}"
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

hit_serializer = AvroSerializer(schema_registry_client, hit_schema_str, to_dict=lambda o, ctx: o)
clickout_serializer = AvroSerializer(schema_registry_client, clickout_schema_str, to_dict=lambda o, ctx: o)

# Producers (we can reuse same underlying client but keep different serializers)
hit_producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "key.serializer": str.encode,
    "value.serializer": hit_serializer,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": secrets["KAFKA_API_KEY"],
    "sasl.password": secrets["KAFKA_API_SECRET"]
}
clickout_producer_conf = {**hit_producer_conf, "value.serializer": clickout_serializer}

hit_producer = SerializingProducer(hit_producer_conf)
clickout_producer = SerializingProducer(clickout_producer_conf)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


# --- Event generators ---

def generate_hit(clickout_id=None):
    return {
        "hit_id": str(uuid.uuid4()),
        "clickout_id": clickout_id,
        "user_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "page": random.choice(["home", "search", "product", "cart"]),
        "timestamp": int(time.time() * 1000)
    }

def generate_clickout(user_id):
    return {
        "clickout_id": str(uuid.uuid4()),
        "user_id": user_id,
        "transaction_id": str(uuid.uuid4()),
        "amount": round(random.uniform(20, 300), 2),
        "currency": "EUR",
        "timestamp": int(time.time() * 1000)
    }


if __name__ == "__main__":
    print("Producing hits and clickouts to Confluent Cloud Kafka...")

    while True:
        # Simulate ~10 hits per second
        for _ in range(10):
            hit = generate_hit()
            hit_producer.produce(
                topic=TOPIC_HITS,
                key=hit["hit_id"],
                value=hit,
                on_delivery=delivery_report
            )

            # With small probability, generate a clickout tied to this hit
            if random.random() < 0.2:  # 20% of hits become clickouts
                clickout = generate_clickout(hit["user_id"])
                # Send clickout
                clickout_producer.produce(
                    topic=TOPIC_CLICKOUTS,
                    key=clickout["clickout_id"],
                    value=clickout,
                    on_delivery=delivery_report
                )
                # Send another hit referencing this clickout_id (to allow joining later)
                hit_with_clickout = generate_hit(clickout_id=clickout["clickout_id"])
                hit_producer.produce(
                    topic=TOPIC_HITS,
                    key=hit_with_clickout["hit_id"],
                    value=hit_with_clickout,
                    on_delivery=delivery_report
                )

        # Flush buffers once per cycle
        hit_producer.flush()
        clickout_producer.flush()
        time.sleep(1)
