import os
from confluent_kafka import Consumer, KafkaError

# Config from environment
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': os.getenv('GROUP_ID', 'dlq-consumer-group'),
    'auto.offset.reset': os.getenv('OFFSET_RESET', 'earliest'),
}

# Topic name from environment
DLQ_TOPIC = os.getenv('DLQ_TOPIC', 'dlq')

# Create consumer
consumer = Consumer(conf)
consumer.subscribe([DLQ_TOPIC])

print(f"📡 Listening to DLQ topic: {DLQ_TOPIC}")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Error: {msg.error()}")
                continue

        print(f"📥 Received: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("\n👋 Exiting...")

finally:
    consumer.close()
