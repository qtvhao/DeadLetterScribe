import { Kafka, logLevel } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

console.log('🚀 Script started');

// Config from environment
const conf = {
  clientId: 'dlq-consumer',
  brokers: process.env.BOOTSTRAP_SERVERS?.split(',') || ['localhost:9092'],
  groupId: process.env.GROUP_ID || 'dlq-consumer-group',
};

const DLQ_TOPIC = process.env.DLQ_TOPIC || 'dlq';

console.log('🔧 Kafka Config:', conf);
console.log('📦 DLQ_TOPIC:', DLQ_TOPIC);

const kafka = new Kafka({
  clientId: conf.clientId,
  brokers: conf.brokers,
  logLevel: logLevel.INFO,
});

// Admin client to check/create topic
const admin = kafka.admin();

async function ensureTopicExists() {
  await admin.connect();
  const topics = await admin.listTopics();

  if (!topics.includes(DLQ_TOPIC)) {
    console.log(`⚠️ Topic '${DLQ_TOPIC}' does not exist. Creating...`);
    try {
      await admin.createTopics({
        topics: [
          {
            topic: DLQ_TOPIC,
            numPartitions: 1,
            replicationFactor: 1,
          },
        ],
      });
      console.log(`✅ Topic '${DLQ_TOPIC}' created successfully.`);
    } catch (e) {
      console.error(`❌ Failed to create topic '${DLQ_TOPIC}':`, e);
      process.exit(1);
    }
  } else {
    console.log(`🔍 Topic '${DLQ_TOPIC}' already exists.`);
  }

  await admin.disconnect();
}

async function startConsumer() {
  const consumer = kafka.consumer({ groupId: conf.groupId });

  try {
    await consumer.connect();
  } catch (e) {
    console.error('❌ Failed to create Kafka consumer:', e);
    process.exit(1);
  }

  try {
    await consumer.subscribe({ topic: DLQ_TOPIC, fromBeginning: conf.autoOffsetReset === 'earliest' });
    console.log(`📱 Listening to DLQ topic: ${DLQ_TOPIC}`);
    console.log('✅ Subscribed. Waiting for messages...');
  } catch (e) {
    console.error('❌ Failed to subscribe to topic:', e);
    process.exit(1);
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value?.toString();
      console.log(`📥 Received from ${topic} [partition ${partition} @ offset ${message.offset}]: ${value}`);
    },
  });
}

// Main execution
(async () => {
  try {
    await ensureTopicExists();
    await startConsumer();
  } catch (e) {
    console.error('🔥 Unhandled Exception:', e);
    process.exit(1);
  }
})();
