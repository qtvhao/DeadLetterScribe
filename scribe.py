import os
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

print("🚀 Script started", flush=True)

# Config from environment
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': os.getenv('GROUP_ID', 'dlq-consumer-group'),
    'auto.offset.reset': os.getenv('OFFSET_RESET', 'earliest'),
}

# Topic name from environment
DLQ_TOPIC = os.getenv('DLQ_TOPIC', 'dlq')

print("🔧 Kafka Config:", conf, flush=True)
print("📦 DLQ_TOPIC:", DLQ_TOPIC, flush=True)

# Check and create topic if it doesn't exist
admin_conf = {'bootstrap.servers': conf['bootstrap.servers']}
admin_client = AdminClient(admin_conf)

topic_metadata = admin_client.list_topics(timeout=5)
if DLQ_TOPIC not in topic_metadata.topics:
    print(f"⚠️ Topic '{DLQ_TOPIC}' does not exist. Creating...", flush=True)
    new_topic = NewTopic(DLQ_TOPIC, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])

    try:
        fs[DLQ_TOPIC].result()
        print(f"✅ Topic '{DLQ_TOPIC}' created successfully.", flush=True)
    except Exception as e:
        print(f"❌ Failed to create topic '{DLQ_TOPIC}': {e}", flush=True)
        exit(1)
else:
    print(f"🔍 Topic '{DLQ_TOPIC}' already exists.", flush=True)

# Try creating the Kafka consumer
try:
    consumer = Consumer(conf)
except Exception as e:
    print(f"❌ Failed to create Kafka consumer: {e}", flush=True)
    exit(1)

# Subscribe to topic
try:
    consumer.subscribe([DLQ_TOPIC])
    print(f"📱 Listening to DLQ topic: {DLQ_TOPIC}", flush=True)
    print("✅ Subscribed. Waiting for messages...", flush=True)
except Exception as e:
    print(f"❌ Failed to subscribe to topic: {e}", flush=True)
    exit(1)

# Start polling loop
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Error: {msg.error()}", flush=True)
                continue

        print(f"📥 Received from {msg.topic()} [partition {msg.partition()} @ offset {msg.offset()}]: {msg.value().decode('utf-8')}", flush=True)

except KeyboardInterrupt:
    print("\n👋 Exiting...", flush=True)

except Exception as e:
    import traceback
    print("🔥 Unhandled Exception:", flush=True)
    traceback.print_exc()

finally:
    consumer.close()
