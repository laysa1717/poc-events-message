import { Kafka, Partitioners } from 'kafkajs';

const broker = process.env.KAFKA_BROKER ?? 'localhost:9092';
const topic = process.env.KAFKA_TOPIC ?? 'poc-events';

async function postTenMessages() {
  const kafka = new Kafka({
    clientId: 'poc-events-producer',
    brokers: [broker],
  });

  const admin = kafka.admin();
  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });
  await admin.connect();
  await producer.connect();

  try {
    const existingTopics = await admin.listTopics();

    if (!existingTopics.includes(topic)) {
      try {
        await admin.createTopics({
          waitForLeaders: true,
          topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
        });
      } catch (error) {
        const topicsAfterCreate = await admin.listTopics();
        if (!topicsAfterCreate.includes(topic)) {
          throw error;
        }
      }
    }

    const messages = Array.from({ length: 10 }, (_, index) => ({
      key: `message-${index + 1}`,
      value: JSON.stringify({
        id: index + 1,
        event: 'health-check-event',
        createdAt: new Date().toISOString(),
      }),
    }));

    let lastError: unknown;

    for (let attempt = 1; attempt <= 5; attempt += 1) {
      try {
        await producer.send({ topic, messages });
        lastError = undefined;
        break;
      } catch (error) {
        lastError = error;
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }

    if (lastError) {
      throw lastError;
    }

    console.log(`10 mensagens enviadas para o topico "${topic}" em ${broker}.`);
  } finally {
    await producer.disconnect();
    await admin.disconnect();
  }
}

postTenMessages().catch((error) => {
  console.error('Falha ao enviar mensagens para o Kafka:', error);
  process.exit(1);
});
