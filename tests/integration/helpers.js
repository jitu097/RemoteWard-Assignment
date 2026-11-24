// Shared setup for integration tests. They talk to a REAL Redis and an S3/SQS endpoint taken from the
// environment. With the default .env.example values that endpoint is the local moto emulator, NOT real AWS.
import { randomUUID } from 'node:crypto';
import { CreateBucketCommand } from '@aws-sdk/client-s3';
import { CreateQueueCommand, PurgeQueueCommand } from '@aws-sdk/client-sqs';
import { loadConfig } from '../../src/config/env.js';
import { createLogger } from '../../src/utils/logger.js';
import { createRedis } from '../../src/services/redis.service.js';
import { createS3Client, createS3Service } from '../../src/services/s3.service.js';
import { createSqsClient, createSqsService } from '../../src/services/sqs.service.js';
import { createJobService } from '../../src/services/job.service.js';

const defaults = {
  AWS_REGION: 'us-east-1',
  AWS_ENDPOINT_URL: 'http://localhost:5000',
  S3_FORCE_PATH_STYLE: 'true',
  AWS_ACCESS_KEY_ID: 'test',
  AWS_SECRET_ACCESS_KEY: 'test',
  REDIS_URL: 'redis://localhost:6379',
};

/** Creates an isolated bucket + queue per test file so runs never interfere. */
export async function setupEnv(overrides = {}) {
  // The SDK reads credentials from process.env; dummy values are only applied for the local emulator.
  if (!process.env.AWS_ENDPOINT_URL || process.env.AWS_ENDPOINT_URL.includes('localhost')) {
    process.env.AWS_ACCESS_KEY_ID ??= defaults.AWS_ACCESS_KEY_ID;
    process.env.AWS_SECRET_ACCESS_KEY ??= defaults.AWS_SECRET_ACCESS_KEY;
  }
  const suffix = randomUUID().slice(0, 8);
  const env = {
    ...defaults,
    ...process.env,
    S3_BUCKET: `it-bucket-${suffix}`,
    SQS_DLQ_URL: `${defaults.AWS_ENDPOINT_URL}/123456789012/it-dlq-${suffix}`,
    SQS_QUEUE_URL: `${defaults.AWS_ENDPOINT_URL}/123456789012/it-queue-${suffix}`,
    ...overrides,
  };
  const config = loadConfig(env);
  const logger = createLogger({ service: 'it', level: 'error', write: () => {} });

  const rawS3 = createS3Client(config);
  const rawSqs = createSqsClient(config);
  await rawS3.send(new CreateBucketCommand({ Bucket: config.s3.bucket }));
  const created = await rawSqs.send(new CreateQueueCommand({ QueueName: config.sqs.queueUrl.split('/').pop() }));
  await rawSqs.send(new PurgeQueueCommand({ QueueUrl: created.QueueUrl })).catch(() => {});

  const redis = createRedis({ url: config.redis.url, logger });
  return {
    config,
    logger,
    redis,
    rawS3,
    s3: createS3Service({ client: rawS3, bucket: config.s3.bucket }),
    queue: createSqsService({ client: rawSqs, queueUrl: created.QueueUrl, visibilityTimeoutSeconds: config.sqs.visibilityTimeoutSeconds }),
    jobs: createJobService({ redis, config }),
    async close() {
      await redis.quit();
    },
  };
}


/** Real Redis only (no AWS endpoint needed). Keys use random job ids, so tests never collide. */
export async function setupRedis(overrides = {}) {
  const config = loadConfig({
    AWS_REGION: 'us-east-1',
    S3_BUCKET: 'unused',
    SQS_QUEUE_URL: 'http://unused/q',
    SQS_DLQ_URL: 'http://unused/dlq',
    REDIS_URL: process.env.REDIS_URL ?? defaults.REDIS_URL,
    ...overrides,
  });
  const logger = createLogger({ service: 'it', level: 'error', write: () => {} });
  const redis = createRedis({ url: config.redis.url, logger });
  return { config, logger, redis, jobs: createJobService({ redis, config }), close: () => redis.quit() };
}
