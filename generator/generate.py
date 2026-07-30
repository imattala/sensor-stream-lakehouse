import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schemas", "sensor_reading.avsc")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.environ.get("TOPIC", "sensor-readings")
NUM_DEVICES = int(os.environ.get("NUM_DEVICES", "5"))
INTERVAL_SECONDS = float(os.environ.get("INTERVAL_SECONDS", "2"))

# Probabilities of injecting each anomaly type per reading, so downstream
# milestones (dead-letter routing, data-quality checks) have real problems to catch.
P_NULL = float(os.environ.get("P_NULL", "0.02"))
P_SPIKE = float(os.environ.get("P_SPIKE", "0.02"))
P_DUPLICATE = float(os.environ.get("P_DUPLICATE", "0.01"))
P_LATE = float(os.environ.get("P_LATE", "0.02"))

METRICS = {
    "TEMPERATURE": {"unit": "celsius", "range": (15.0, 30.0)},
    "HUMIDITY": {"unit": "percent", "range": (30.0, 70.0)},
    "VIBRATION": {"unit": "mm/s", "range": (0.0, 5.0)},
    "BATTERY": {"unit": "percent", "range": (20.0, 100.0)},
}

DEVICE_IDS = [f"device-{i:03d}" for i in range(1, NUM_DEVICES + 1)]


def now_millis():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def make_reading(device_id, metric):
    unit = METRICS[metric]["unit"]
    lo, hi = METRICS[metric]["range"]
    value = round(random.uniform(lo, hi), 2)
    timestamp = now_millis()
    anomaly = None

    roll = random.random()
    if roll < P_NULL:
        value = None
        anomaly = "null_value"
    elif roll < P_NULL + P_SPIKE:
        value = round(hi * random.uniform(3, 8), 2)
        anomaly = "out_of_range_spike"
    elif roll < P_NULL + P_SPIKE + P_LATE:
        timestamp = now_millis() - random.randint(5, 30) * 60_000
        anomaly = "late_event"

    reading = {
        "reading_id": str(uuid.uuid4()),
        "device_id": device_id,
        "metric": metric,
        "value": value,
        "unit": unit,
        "timestamp": timestamp,
    }
    return reading, anomaly


def delivery_report(err, msg):
    if err is not None:
        print(f"[delivery-failed] {err}")


def main():
    with open(SCHEMA_PATH) as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    producer = SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
    })

    print(f"Producing to '{TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS} for devices {DEVICE_IDS}")

    while True:
        for device_id in DEVICE_IDS:
            for metric in METRICS:
                reading, anomaly = make_reading(device_id, metric)
                producer.produce(topic=TOPIC, key=device_id, value=reading, on_delivery=delivery_report)

                if anomaly:
                    print(f"[anomaly:{anomaly}] {reading}")

                if random.random() < P_DUPLICATE:
                    # Resend the exact same reading (same reading_id) to simulate a duplicate delivery.
                    producer.produce(topic=TOPIC, key=device_id, value=reading, on_delivery=delivery_report)
                    print(f"[anomaly:duplicate] {reading}")

        producer.poll(0)
        producer.flush()
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
