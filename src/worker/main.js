import http from 'node:http';
import { loadConfig } from '../config/env.js';
import { createLogger } from '../utils/logger.js';
import { createRedis } from '../services/redis.service.js';
import { createS3Client, createS3Service } from '../services/s3.service.js';
import { createSqsClient, createSqsService } from '../services/sqs.service.js';
import { createJobService } from '../services/job.service.js';
import { createConsumer } from './consumer.js';

const config = loadConfig();
const logger = createLogger({ service: 'worker', level: config.logLevel });
const redis = createRedis({ url: config.redis.url, logger });

const consumer = createConsumer({
  config,
  logger,
  queue: createSqsService({
    client: createSqsClient(config),
    queueUrl: config.sqs.queueUrl,
    dlqUrl: config.sqs.dlqUrl,
    visibilityTimeoutSeconds: config.sqs.visibilityTimeoutSeconds,
  }),
  s3: createS3Service({ client: createS3Client(config), bucket: config.s3.bucket }),
  jobs: createJobService({ redis, config }),
  isReady: () => redis.status === 'ready',
});

// Liveness reflects the process only (event loop + poll loop still turning), never AWS/Redis availability,
// so a dependency outage does not make Kubernetes restart the pod. Readiness is informational here.
const STALL_MS = 120000;
const health = http.createServer((req, res) => {
  const alive = Date.now() - consumer.lastLoopAt < STALL_MS;
  if (req.url === '/healthz') {
    res.writeHead(alive ? 200 : 503).end(alive ? 'ok' : 'poll loop stalled');
  } else if (req.url === '/readyz') {
    const ready = !consumer.stopping;
    res.writeHead(ready ? 200 : 503, { 'content-type': 'application/json' }).end(JSON.stringify({ ready, redis: redis.status }));
  } else {
    res.writeHead(404).end();
  }
});
health.listen(config.worker.healthPort);

let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info('worker.signal_received', { signal });
  await consumer.stop();
  health.close();
  await redis.quit().catch(() => {});
  process.exit(0);
}
for (const s of ['SIGTERM', 'SIGINT']) process.on(s, () => shutdown(s));

consumer.start();
