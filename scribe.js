import { Kafka } from 'kafkajs';
import * as fs from 'fs';
import path from 'path';

const kafka = new Kafka({
  clientId: 'dlq-consumer',
  brokers: (process.env.BOOTSTRAP_SERVERS || 'localhost:9092').split(','),
});

const topic = 'dlq';
const debugTopic = 'dlq-debug-data';
const groupId = 'dlq-group';

(async () => {
  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();

  // Ensure 'dlq' topic exists
  if (!topics.includes(topic)) {
    await admin.createTopics({
      topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
    });
  }

  // Ensure 'dlq-debug-data' topic exists
  if (!topics.includes(debugTopic)) {
    await admin.createTopics({
      topics: [{ topic: debugTopic, numPartitions: 1, replicationFactor: 1 }],
    });
  }

  await admin.disconnect();

  // Consumer for 'dlq'
  const dlqConsumer = kafka.consumer({ groupId });
  await dlqConsumer.connect();
  await dlqConsumer.subscribe({ topic, fromBeginning: true });

  dlqConsumer.run({
    eachMessage: async ({ message }) => {
      console.log(`[DLQ] ${message.value?.toString()}`);
    },
  });

  // Consumer for 'dlq-debug-data'
  const debugConsumer = kafka.consumer({ groupId: 'dlq-debug-group' });
  await debugConsumer.connect();
  await debugConsumer.subscribe({ topic: debugTopic, fromBeginning: true });

  debugConsumer.run({
    eachMessage: async ({ message }) => {
      const msg = message.value?.toString();
      console.log(msg)
      if (msg) {
        const filePath = path.join('/tmp', `dlq-debug-${Date.now()}.log`);
        fs.writeFile(filePath, msg, (err) => {
          if (err) console.error(`Failed to write file: ${err}`);
          else console.log(`[DLQ-DEBUG] Wrote message to ${filePath}`);
        });
      }
    },
  });
})();
