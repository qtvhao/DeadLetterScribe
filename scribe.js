import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'dlq-consumer',
  brokers: (process.env.BOOTSTRAP_SERVERS || 'localhost:9092').split(','),
});

(async () => {
  const consumer = kafka.consumer({ groupId: 'dlq-group' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'dlq', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log(message.value?.toString());
    },
  });
})();
