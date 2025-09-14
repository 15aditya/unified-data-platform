import time
import uuid
import random
import json
from confluent_kafka import Producer

KAFKA_BROKER = ""   # or Confluent Cloud
TOPIC = "users"

conf = {
    "bootstrap.servers": KAFKA_BROKER
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def generate_cdc_event(op="c"):
    txn_id = str(uuid.uuid4())
    event = {
        "payload": {
            "before": None if op == "c" else {"id": txn_id, "amount": 20.0},
            "after": {
                "id": txn_id,
                "user_id": str(uuid.uuid4()),
                "amount": round(random.uniform(10, 200), 2),
                "currency": "EUR"
            } if op != "d" else None,
            "op": op,
            "ts_ms": int(time.time() * 1000)
        }
    }
    return event

if __name__ == "__main__":
    ops = ["c", "u", "d"]
    while True:
        event = generate_cdc_event(random.choice(ops))
        producer.produce(
            topic=TOPIC,
            key=event["payload"]["after"]["id"] if event["payload"]["after"] else str(uuid.uuid4()),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(0.5)
