#!/usr/bin/env python3
"""
Quick test to consume from metrics_flattened topic
"""
import os
from dotenv import load_dotenv
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer

load_dotenv()

# Get credentials from config
from config import load_kafka_config
kafka_config = load_kafka_config()

kafka_api_key = os.getenv("KAFKA_API_KEY")
kafka_api_secret = os.getenv("KAFKA_API_SECRET")
bootstrap_servers = kafka_config["bootstrap_servers"]

# Configure consumer
consumer_conf = {
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": kafka_api_key,
    "sasl.password": kafka_api_secret,
    "group.id": "test-consumer",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["metrics_flattened"])

print("Consuming from metrics_flattened topic...")
print("Waiting for messages (Ctrl+C to stop)...\n")

try:
    message_count = 0
    while message_count < 5:
        msg = consumer.poll(timeout=5.0)

        if msg is None:
            print("No more messages, exiting...")
            break

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        message_count += 1
        # Value is Schema Registry encoded, just check size
        print(f"Message {message_count}:")
        print(f"  Key: {msg.key().decode('utf-8') if msg.key() else None}")
        print(f"  Value size: {len(msg.value())} bytes")
        print(f"  Partition: {msg.partition()}, Offset: {msg.offset()}")
        print()

except KeyboardInterrupt:
    print("\nStopped by user")
finally:
    consumer.close()
    print(f"\nConsumed {message_count} messages")
