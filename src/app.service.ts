import { Injectable } from '@nestjs/common';
import { Kafka, Partitioners } from 'kafkajs';

@Injectable()
export class AppService {
  private readonly topicPartitions = 9;
  private readonly consumerConcurrency = 9;

  private async ensureTopicExists(
    admin: ReturnType<Kafka['admin']>,
    topic: string,
    targetPartitions: number,
  ) {
    const existingTopics = await admin.listTopics();

    if (!existingTopics.includes(topic)) {
      try {
        await admin.createTopics({
          waitForLeaders: true,
          topics: [
            {
              topic,
              numPartitions: targetPartitions,
              replicationFactor: 1,
            },
          ],
        });
      } catch (error) {
        const topicsAfterCreate = await admin.listTopics();
        if (!topicsAfterCreate.includes(topic)) {
          throw error;
        }
      }
    }

    const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
    const currentPartitions = metadata.topics[0]?.partitions.length ?? 0;

    if (currentPartitions < targetPartitions) {
      await admin.createPartitions({
        topicPartitions: [{ topic, count: targetPartitions }],
        validateOnly: false,
      });
    }
  }

  getHealth() {
    return {
      status: 'ok',
      timestamp: new Date().toISOString(),
    };
  }

  async publishTenMessages() {
    const broker = process.env.KAFKA_BROKER ?? 'localhost:9092';
    const topic = process.env.KAFKA_TOPIC ?? 'poc-events';

    const kafka = new Kafka({
      clientId: 'poc-events-api-producer',
      brokers: [broker],
    });

    const admin = kafka.admin();
    const producer = kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
    await admin.connect();
    await producer.connect();

    try {
      await this.ensureTopicExists(admin, topic, this.topicPartitions);

      const messages = Array.from({ length: 10 }, (_, index) => ({
        key: `message-${index + 1}`,
        value: JSON.stringify({
          id: index + 1,
          event: 'api-post-event',
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

      return {
        status: 'ok',
        sent: 10,
        topic,
        broker,
        timestamp: new Date().toISOString(),
      };
    } finally {
      await producer.disconnect();
      await admin.disconnect();
    }
  }

  async consumeTopicAndReport() {
    const broker = process.env.KAFKA_BROKER ?? 'localhost:9092';
    const topic = process.env.KAFKA_TOPIC ?? 'poc-events';
    const consumerGroupId =
      process.env.KAFKA_CONSUMER_GROUP_ID ?? 'poc-events-consumer-group';
    const kafka = new Kafka({
      clientId: 'poc-events-api-consumer',
      brokers: [broker],
    });

    const admin = kafka.admin();
    await admin.connect();

    try {
      await this.ensureTopicExists(admin, topic, this.topicPartitions);

      const partitionOffsets = await admin.fetchTopicOffsets(topic);
      const totalToConsume = partitionOffsets.reduce(
        (acc, partition) => acc + Number(partition.offset),
        0,
      );

      const consumer = kafka.consumer({
        groupId: consumerGroupId,
      });
      await consumer.connect();
      try {
        await consumer.subscribe({ topic, fromBeginning: true });

        const startedAtMs = Date.now();
        let consumed = 0;
        let stopping = false;
        let lastMessageAtMs = Date.now();
        const idleMs = 2000;
        const maxDurationMs = 60000;

        await new Promise<void>((resolve, reject) => {
          const maxDurationTimer = setTimeout(() => {
            if (stopping) {
              return;
            }
            stopping = true;
            consumer
              .stop()
              .then(() =>
                reject(
                  new Error(
                    'Timeout ao consumir mensagens do topico (tempo maximo excedido).',
                  ),
                ),
              )
              .catch(reject);
          }, maxDurationMs);

          const idleCheck = setInterval(() => {
            if (stopping) {
              return;
            }

            const noMessagesRecently = Date.now() - lastMessageAtMs >= idleMs;
            if (noMessagesRecently) {
              stopping = true;
              consumer
                .stop()
                .then(() => {
                  clearInterval(idleCheck);
                  clearTimeout(maxDurationTimer);
                  resolve();
                })
                .catch((error) => {
                  clearInterval(idleCheck);
                  clearTimeout(maxDurationTimer);
                  reject(error);
                });
            }
          }, 200);

          consumer
            .run({
              partitionsConsumedConcurrently: this.consumerConcurrency,
              eachMessage: async () => {
                consumed += 1;
                lastMessageAtMs = Date.now();
              },
            })
            .catch((error) => {
              clearInterval(idleCheck);
              clearTimeout(maxDurationTimer);
              reject(error);
            });
        });

        const finishedAtMs = Date.now();

        return {
          status: 'ok',
          topic,
          broker,
          consumerGroupId,
          partitions: partitionOffsets.length,
          consumerConcurrency: this.consumerConcurrency,
          totalToConsume,
          consumed,
          durationMs: finishedAtMs - startedAtMs,
          startedAt: new Date(startedAtMs).toISOString(),
          finishedAt: new Date(finishedAtMs).toISOString(),
        };
      } finally {
        await consumer.disconnect();
      }
    } finally {
      await admin.disconnect();
    }
  }
}
