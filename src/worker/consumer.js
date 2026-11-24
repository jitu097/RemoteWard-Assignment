import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { PermanentError } from '../utils/errors.js';
import { validateEvent } from '../utils/events.js';
import { processStream } from './processor.js';

class AbortedError extends Error {
  constructor(message) {
    super(message);
    this.name = 'AbortedError';
  }
}

const MAX_BATCH = 10; // SQS limit per receive call
const MAX_BACKOFF_MS = 30000;

/**
 * SQS consumer. At-least-once delivery means every message may arrive more than once, so each job is
 * guarded by a Redis lock and a terminal `completed` state; the message is deleted only after the outcome
 * (result, or a recorded permanent failure) is safely stored.
 *
 * Message outcomes (return value of handleMessage):
 *   completed          result stored, message deleted
 *   duplicate          job already completed, message deleted
 *   locked             another worker holds the job; message left for redelivery
 *   permanent_failure  failure recorded, copy sent to DLQ, message deleted
 *   transient_failure  failure recorded if possible; message left so SQS redelivers it (redrive policy -> DLQ)
 *   released           aborted (shutdown / lost lock); message left, state untouched
 */
export function createConsumer({ config, logger, queue, s3, jobs, isReady = () => true, processFile = processStream }) {
  const inFlight = new Set();
  const pollAbort = new AbortController();
  const taskAbort = new AbortController();
  let stopping = false;
  let lastLoopAt = Date.now();
  let loop;

  async function deleteMessage(message, jobId) {
    try {
      await queue.delete(message.ReceiptHandle);
    } catch (err) {
      // Harmless: the redelivered message will hit the `completed` state and be deleted then.
      logger.warn('worker.delete_failed', { jobId, err });
    }
  }

  async function rejectPermanently(message, jobId, err) {
    try {
      if (jobId) {
        await jobs.fail(jobId, { code: 'PERMANENT', permanent: true, message: String(err.message).slice(0, 300), at: new Date().toISOString() });
      }
      await queue.sendToDlq(message.Body, err.message);
      await deleteMessage(message, jobId);
      logger.error('worker.permanent_failure', { jobId, err });
      return 'permanent_failure';
    } catch (persistErr) {
      // Could not record the outcome: leave the message so it is retried and eventually dead-lettered.
      logger.error('worker.permanent_failure_not_recorded', { jobId, err: persistErr });
      return 'transient_failure';
    }
  }

  async function handleMessage(message, signal = taskAbort.signal) {
    let event;
    try {
      let body;
      try {
        body = JSON.parse(message.Body);
      } catch {
        throw new PermanentError('Message body is not valid JSON');
      }
      event = validateEvent(body);
    } catch (err) {
      return rejectPermanently(message, null, err);
    }

    const { jobId } = event;
    const token = randomUUID();
    const ctx = { jobId, receiveCount: message.Attributes?.ApproximateReceiveCount };
    let locked = false;
    let heartbeat;
    const jobAbort = new AbortController();
    const jobSignal = AbortSignal.any([signal, jobAbort.signal]);

    try {
      locked = await jobs.acquireLock(jobId, token);
      if (!locked) {
        logger.info('worker.job_locked', ctx);
        return 'locked';
      }

      const job = await jobs.get(jobId);
      if (!job) throw new PermanentError('Unknown job: status record is missing or expired');
      if (job.status === 'completed') {
        logger.info('worker.duplicate_ignored', ctx);
        await deleteMessage(message, jobId);
        return 'duplicate';
      }

      const started = await jobs.markProcessing(jobId);
      if (!started.ok) {
        if (started.current === 'completed') {
          await deleteMessage(message, jobId);
          return 'duplicate';
        }
        throw new Error(`Cannot start job in state "${started.current}"`);
      }
      logger.info('worker.processing_started', { ...ctx, attempt: started.record.attempts });

      // Keep the lock and the message invisible for as long as processing runs.
      heartbeat = setInterval(async () => {
        try {
          if (await jobs.renewLock(jobId, token)) await queue.extendVisibility(message.ReceiptHandle);
          else jobAbort.abort(new AbortedError('Lost the job lock'));
        } catch (err) {
          logger.warn('worker.heartbeat_failed', { ...ctx, err });
        }
      }, (config.jobs.lockTtlSeconds * 1000) / 3);

      const body = await s3.download({ bucket: event.file.bucket, key: event.file.key });
      const result = await processFile(body, event.file.contentType, jobSignal);

      const done = await jobs.complete(jobId, result);
      if (!done.ok && done.current !== 'completed') throw new Error(`Cannot complete job in state "${done.current}"`);
      await deleteMessage(message, jobId); // only after the result is persisted
      logger.info(done.ok ? 'worker.job_completed' : 'worker.duplicate_ignored', { ...ctx, totalRecords: result.totalRecords });
      return done.ok ? 'completed' : 'duplicate';
    } catch (err) {
      if (jobSignal.aborted) {
        logger.warn('worker.job_released', { ...ctx, reason: jobSignal.reason?.message });
        return 'released';
      }
      if (err instanceof PermanentError || err?.name === 'NoSuchKey') return rejectPermanently(message, jobId, err);

      logger.error('worker.transient_failure', { ...ctx, err });
      await jobs
        .fail(jobId, { code: 'TRANSIENT', permanent: false, retryable: true, message: String(err.message).slice(0, 300), at: new Date().toISOString() })
        .catch((e) => logger.warn('worker.failure_not_recorded', { ...ctx, err: e }));
      return 'transient_failure';
    } finally {
      clearInterval(heartbeat);
      if (locked) await jobs.releaseLock(jobId, token).catch((err) => logger.warn('worker.unlock_failed', { ...ctx, err }));
    }
  }

  async function pollLoop() {
    let backoff = 0;
    while (!stopping) {
      lastLoopAt = Date.now();
      const free = config.worker.concurrency - inFlight.size;
      if (free <= 0) {
        await Promise.race(inFlight);
        continue;
      }
      // While Redis is down every message would fail and burn receives towards the DLQ, so don't poll.
      if (!isReady()) {
        await sleep(1000, undefined, { signal: pollAbort.signal }).catch(() => {});
        continue;
      }
      let messages;
      try {
        messages = await queue.receive({ max: Math.min(free, MAX_BATCH), abortSignal: pollAbort.signal });
        backoff = 0;
      } catch (err) {
        if (stopping) break;
        backoff = Math.min(backoff * 2 || 1000, MAX_BACKOFF_MS);
        logger.error('worker.receive_failed', { err, retryInMs: backoff });
        await sleep(backoff, undefined, { signal: pollAbort.signal }).catch(() => {});
        continue;
      }
      for (const message of messages) {
        const task = handleMessage(message).finally(() => inFlight.delete(task));
        inFlight.add(task);
      }
    }
  }

  return {
    handleMessage,
    start() {
      logger.info('worker.started', { concurrency: config.worker.concurrency });
      loop = pollLoop();
      return loop;
    },
    /** Stops polling, lets in-flight jobs finish within the shutdown timeout, then releases the rest. */
    async stop() {
      if (stopping) return;
      stopping = true;
      logger.info('worker.stopping', { inFlight: inFlight.size });
      pollAbort.abort();
      const drained = (async () => {
        await loop; // the loop may be waiting for a free slot, so it counts towards the deadline too
        await Promise.allSettled([...inFlight]);
      })();
      const timedOut = await Promise.race([drained.then(() => false), sleep(config.worker.shutdownTimeoutMs, undefined, { ref: false }).then(() => true)]);
      if (timedOut) {
        logger.warn('worker.shutdown_timeout', { abandoned: inFlight.size });
        taskAbort.abort(new AbortedError('Worker shutting down'));
        await Promise.allSettled([...inFlight]);
      }
      logger.info('worker.stopped');
    },
    get stopping() {
      return stopping;
    },
    get lastLoopAt() {
      return lastLoopAt;
    },
  };
}
