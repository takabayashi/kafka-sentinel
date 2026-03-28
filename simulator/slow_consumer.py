#!/usr/bin/env python3
"""
Slow consumer to create lag for testing velocity-monitor.
Consumes from simulator_events at a slow rate to create persistent lag.
"""
import os
import sys
import time
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

# Consumer config
consumer_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': 'test-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 5000,
}

consumer = Consumer(consumer_config)
consumer.subscribe(['simulator_events'])

print("Slow consumer started for group 'test-consumer'", flush=True)
print("Consuming at 10 messages/second to create lag...", flush=True)

message_count = 0
try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}", flush=True)
                break

        message_count += 1
        if message_count % 100 == 0:
            print(f"Consumed {message_count} messages (slow rate to create lag)", flush=True)

        # Consume slowly - 10 msg/s
        time.sleep(0.1)

except KeyboardInterrupt:
    print(f"\nStopping consumer. Total messages consumed: {message_count}", flush=True)
finally:
    consumer.close()
