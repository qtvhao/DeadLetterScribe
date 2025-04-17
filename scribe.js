import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'dlq-consumer',
  brokers: (process.env.BOOTSTRAP_SERVERS || 'localhost:9092').split(','),
});

const topic = 'dlq';
const groupId = 'dlq-group';

(async () => {
  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();
  if (!topics.includes(topic)) {
    await admin.createTopics({
      topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
    });
  }
  await admin.disconnect();

  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log(message.value?.toString());
    },
  });
})();
