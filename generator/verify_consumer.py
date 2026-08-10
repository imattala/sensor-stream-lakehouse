"""Manual smoke-test consumer for M1: prints the next N decoded sensor readings.

Usage: python verify_consumer.py [count]
"""
import os
import sys

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.environ.get("TOPIC", "sensor-readings")


def main():
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 20

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer = DeserializingConsumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": avro_deserializer,
        "group.id": "verify-consumer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    print(f"Consuming up to {count} messages from '{TOPIC}'...")
    seen = 0
    try:
        while seen < count:
            msg = consumer.poll(5.0)
            if msg is None:
                print("(no message within 5s, still waiting...)")
                continue
            if msg.error():
                print(f"[error] {msg.error()}")
                continue
            print(f"key={msg.key()} value={msg.value()}")
            seen += 1
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
