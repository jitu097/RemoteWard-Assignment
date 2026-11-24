import { loadConfig } from '../config/env.js';
import { createLogger } from '../utils/logger.js';
import { createRedis } from '../services/redis.service.js';
import { createS3Client, createS3Service } from '../services/s3.service.js';
import { createSqsClient, createSqsService } from '../services/sqs.service.js';
import { createJobService } from '../services/job.service.js';
import { createApp } from './app.js';
import { createRateLimiters } from './middleware/rate-limit.js';

const config = loadConfig();
const logger = createLogger({ service: 'api', level: config.logLevel });
const redis = createRedis({ url: config.redis.url, logger });

const app = createApp({
  config,
  logger,
  s3: createS3Service({ client: createS3Client(config), bucket: config.s3.bucket }),
  queue: createSqsService({ client: createSqsClient(config), queueUrl: config.sqs.queueUrl, visibilityTimeoutSeconds: config.sqs.visibilityTimeoutSeconds }),
  jobs: createJobService({ redis, config }),
  limiters: createRateLimiters({ redis, config, logger }),
});

const server = app.listen(config.port, () => logger.info('api.started', { port: config.port }));

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
    logger.info('api.shutdown', { signal });
    server.close(() => {
      redis.quit().finally(() => process.exit(0));
    });
    setTimeout(() => process.exit(1), 10000).unref();
  });
}
