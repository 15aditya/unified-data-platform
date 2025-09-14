import time
import uuid
import random
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from aws_secrets import get_secret

secrets = get_secret("workshop/unified_data_platform/confluent_kafka_keys")

# Confluent Cloud Kafka configs
KAFKA_BROKER = "pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092"
TOPIC = "transactions"

# Schema Registry configs
SCHEMA_REGISTRY_URL = "https://psrc-8vyvr.eu-central-1.aws.confluent.cloud"

# Avro schema
avro_schema_str = """
{
  "namespace": "com.example",
  "name": "Transaction",
  "type": "record",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "clickout_id", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

# Schema Registry client
schema_registry_conf = {
    "url": SCHEMA_REGISTRY_URL,
    "basic.auth.user.info": f"{secrets['SCHEMA_REGISTRY_API_KEY']}:{secrets['SCHEMA_REGISTRY_API_SECRET']}"
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro serializer
avro_serializer = AvroSerializer(
    schema_registry_client,
    avro_schema_str,
    to_dict=lambda obj, ctx: obj
)

# Kafka producer config
producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "key.serializer": str.encode,
    "value.serializer": avro_serializer,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": secrets["KAFKA_API_KEY"],
    "sasl.password": secrets["KAFKA_API_SECRET"]
}

producer = SerializingProducer(producer_conf)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered message to {msg.topic()} [{msg.partition()}]")

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "clickout_id": str(uuid.uuid4()),
        "currency": "EUR",
        "user_id": str(uuid.uuid4()),
        "amount": round(random.uniform(10, 500), 2),
        "timestamp": int(time.time() * 1000)
    }

if __name__ == "__main__":
    print("Producing 100 events/sec to Confluent Cloud Kafka...")
    while True:
        start_time = time.time()
        for _ in range(100):
            txn = generate_transaction()
            producer.produce(
                topic=TOPIC,
                key=txn["transaction_id"],
                value=txn,
                on_delivery=delivery_report
            )
        producer.flush()
        elapsed = time.time() - start_time
        time.sleep(max(0, 1 - elapsed))
