// Recovery tool: re-publishes the event for a job whose upload reached S3 but whose SQS publish failed.
// Usage: node scripts/requeue-job.js <jobId>
import { loadConfig } from '../src/config/env.js';
import { createLogger } from '../src/utils/logger.js';
import { createRedis } from '../src/services/redis.service.js';
import { createSqsClient, createSqsService } from '../src/services/sqs.service.js';
import { createJobService } from '../src/services/job.service.js';
import { buildFileUploadedEvent, isJobId } from '../src/utils/events.js';

const jobId = process.argv[2];
if (!isJobId(jobId)) {
  console.error('Usage: node scripts/requeue-job.js <jobId>');
  process.exit(2);
}

const config = loadConfig();
const logger = createLogger({ service: 'requeue', level: config.logLevel });
const redis = createRedis({ url: config.redis.url, logger });
const jobs = createJobService({ redis, config });
const queue = createSqsService({ client: createSqsClient(config), queueUrl: config.sqs.queueUrl, visibilityTimeoutSeconds: config.sqs.visibilityTimeoutSeconds });

let code = 0;
try {
  const job = await jobs.get(jobId);
  if (!job) throw new Error('Job not found (status expired?). Locate the object via the upload.enqueue_failed log line.');
  if (job.status !== 'enqueue_failed') throw new Error(`Job is "${job.status}"; only enqueue_failed jobs can be re-queued`);
  // Flip state first so a fast worker never sees enqueue_failed; undo if the publish fails again.
  const flipped = await jobs.requeue(jobId);
  if (!flipped.ok) throw new Error('Job changed state concurrently; re-check its status');
  try {
    await queue.publish(buildFileUploadedEvent({ jobId, ...job.file, uploadedAt: new Date(job.createdAt) }));
  } catch (err) {
    await jobs.markEnqueueFailed(jobId, { code: 'ENQUEUE_FAILED', message: String(err.message).slice(0, 500) });
    throw err;
  }
  console.log(`Job ${jobId} re-queued.`);
} catch (err) {
  console.error(err.message);
  code = 1;
} finally {
  await redis.quit();
}
process.exit(code);
