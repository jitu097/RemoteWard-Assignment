// Worker behaviour with a REAL Redis and FAKE S3/SQS (in-memory). Tests message handling, locking,
// idempotency, retry classification and shutdown. It does not verify real AWS.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { Readable } from 'node:stream';
import { setTimeout as sleep } from 'node:timers/promises';
import { setupRedis } from './helpers.js';
import { createConsumer } from '../../src/worker/consumer.js';
import { buildFileUploadedEvent } from '../../src/utils/events.js';
import { keys } from '../../src/services/job.service.js';

let env;
before(async () => {
  env = await setupRedis({ JOB_LOCK_TTL_SECONDS: '5', SQS_VISIBILITY_TIMEOUT_SECONDS: '30' });
});
after(() => env.close());

function fakeQueue() {
  const q = { deleted: [], dlq: [], pending: [], log: [] };
  q.delete = async (h) => { q.log.push('delete'); q.deleted.push(h); };
  q.sendToDlq = async (body, reason) => { q.dlq.push({ body, reason }); };
  q.extendVisibility = async () => {};
  q.receive = ({ max, abortSignal }) =>
    q.pending.length
      ? Promise.resolve(q.pending.splice(0, max))
      : new Promise((_, reject) => abortSignal.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), { name: 'AbortError' }))));
  return q;
}

const fixtureS3 = (path = 'tests/fixtures/customers.csv') => ({ download: async () => createReadStream(path) });

function build({ s3 = fixtureS3(), jobs = env.jobs, queue = fakeQueue(), ...opts } = {}, workerOverrides = {}) {
  const config = { ...env.config, worker: { ...env.config.worker, concurrency: 2, shutdownTimeoutMs: 2000, ...workerOverrides } };
  return { queue, consumer: createConsumer({ config, logger: env.logger, queue, s3, jobs, ...opts }) };
}

async function seed({ contentType = 'text/csv', create = true } = {}) {
  const jobId = randomUUID();
  const file = { bucket: 'b', key: `uploads/2026/11/24/${jobId}.csv`, originalName: 'c.csv', contentType, size: 10 };
  if (create) await env.jobs.create(jobId, file);
  const message = { ReceiptHandle: `rh-${jobId}`, Body: JSON.stringify(buildFileUploadedEvent({ jobId, ...file })), Attributes: { ApproximateReceiveCount: '1' } };
  return { jobId, message };
}

test('success: result persisted, job completed, message deleted, lock released', async () => {
  const { queue, consumer } = build();
  const { jobId, message } = await seed();
  assert.equal(await consumer.handleMessage(message), 'completed');
  assert.equal((await env.jobs.get(jobId)).status, 'completed');
  assert.equal((await env.jobs.getResult(jobId)).totalRecords, 4);
  assert.deepEqual(queue.deleted, [message.ReceiptHandle]);
  assert.equal(await env.redis.exists(keys.lock(jobId)), 0);
});

test('message is deleted only after the result is persisted', async () => {
  const queue = fakeQueue();
  const jobs = { ...env.jobs, complete: async (...a) => { const r = await env.jobs.complete(...a); queue.log.push('persist'); return r; } };
  const { consumer } = build({ jobs, queue });
  const { message } = await seed();
  await consumer.handleMessage(message);
  assert.deepEqual(queue.log, ['persist', 'delete']);
});

test('Redis persistence failure: message NOT deleted, no result, failure recorded, retry succeeds', async () => {
  const queue = fakeQueue();
  let fail = true;
  const jobs = { ...env.jobs, complete: async (...a) => { if (fail) throw new Error('Redis write failed'); return env.jobs.complete(...a); } };
  const { consumer } = build({ jobs, queue });
  const { jobId, message } = await seed();
  assert.equal(await consumer.handleMessage(message), 'transient_failure');
  assert.deepEqual(queue.deleted, []);
  assert.equal(await env.jobs.getResult(jobId), null);
  assert.equal((await env.jobs.get(jobId)).status, 'failed');
  fail = false;
  assert.equal(await consumer.handleMessage(message), 'completed');
  assert.deepEqual(queue.deleted, [message.ReceiptHandle]);
});

test('S3 download failure is transient: recorded, message kept, later retry completes', async () => {
  let broken = true;
  const s3 = { download: async () => { if (broken) throw Object.assign(new Error('connection reset'), { name: 'TimeoutError' }); return createReadStream('tests/fixtures/customers.csv'); } };
  const { queue, consumer } = build({ s3 });
  const { jobId, message } = await seed();
  assert.equal(await consumer.handleMessage(message), 'transient_failure');
  const failed = await env.jobs.get(jobId);
  assert.equal(failed.status, 'failed');
  assert.equal(failed.error.retryable, true);
  assert.deepEqual([queue.deleted.length, queue.dlq.length], [0, 0]);
  assert.equal(await env.redis.exists(keys.lock(jobId)), 0, 'lock released so the retry can run');

  broken = false;
  assert.equal(await consumer.handleMessage(message), 'completed');
  const done = await env.jobs.get(jobId);
  assert.deepEqual([done.status, done.attempts], ['completed', 2]);
});

test('missing S3 object (NoSuchKey) is permanent: failed, copied to DLQ, deleted', async () => {
  const s3 = { download: async () => { throw Object.assign(new Error('missing'), { name: 'NoSuchKey' }); } };
  const { queue, consumer } = build({ s3 });
  const { jobId, message } = await seed();
  assert.equal(await consumer.handleMessage(message), 'permanent_failure');
  const job = await env.jobs.get(jobId);
  assert.deepEqual([job.status, job.error.permanent], ['failed', true]);
  assert.equal(queue.dlq.length, 1);
  assert.deepEqual(queue.deleted, [message.ReceiptHandle]);
});

test('malformed file content is a permanent failure', async () => {
  const s3 = { download: async () => Readable.from(['a,b\n1\n']) };
  const { queue, consumer } = build({ s3 });
  const { jobId, message } = await seed();
  assert.equal(await consumer.handleMessage(message), 'permanent_failure');
  assert.match((await env.jobs.get(jobId)).error.message, /Malformed input/);
  assert.equal(queue.dlq.length, 1);
});

for (const [name, body] of [
  ['not JSON', 'hello'],
  ['unsupported version', JSON.stringify({ eventType: 'FILE_UPLOADED', eventVersion: 9 })],
  ['missing fields', JSON.stringify({ eventType: 'FILE_UPLOADED', eventVersion: 1, jobId: 'x' })],
]) {
  test(`malformed event (${name}): sent to DLQ and deleted, nothing processed`, async () => {
    let downloads = 0;
    const { queue, consumer } = build({ s3: { download: async () => { downloads++; } } });
    assert.equal(await consumer.handleMessage({ ReceiptHandle: 'r', Body: body }), 'permanent_failure');
    assert.deepEqual([queue.dlq.length, queue.deleted.length, downloads], [1, 1, 0]);
  });
}

test('event for an unknown job is permanent', async () => {
  const { queue, consumer } = build();
  const { message } = await seed({ create: false });
  assert.equal(await consumer.handleMessage(message), 'permanent_failure');
  assert.equal(queue.dlq.length, 1);
});

test('duplicate delivery after completion: not reprocessed, result untouched, message deleted', async () => {
  let runs = 0;
  const { queue, consumer } = build({ processFile: async () => { runs++; return { totalRecords: runs }; } });
  const { jobId, message } = await seed();
  assert.equal(await consumer.handleMessage(message), 'completed');
  assert.equal(await consumer.handleMessage(message), 'duplicate');
  assert.equal(runs, 1);
  assert.deepEqual(await env.jobs.getResult(jobId), { totalRecords: 1 });
  assert.equal(queue.deleted.length, 2);
});

test('concurrent duplicate delivery: exactly one worker processes, the other backs off', async () => {
  let runs = 0;
  const processFile = async () => { runs++; await sleep(200); return { totalRecords: 1 }; };
  const a = build({ processFile });
  const b = build({ processFile });
  const { jobId, message } = await seed();
  const outcomes = await Promise.all([a.consumer.handleMessage(message), b.consumer.handleMessage(message)]);
  assert.deepEqual(outcomes.sort(), ['completed', 'locked']);
  assert.equal(runs, 1);
  const lockedSide = [a, b].find((_, i) => outcomes[i] === 'locked');
  assert.deepEqual(lockedSide.queue.deleted, [], 'a backed-off duplicate must not delete the message');
  assert.equal((await env.jobs.get(jobId)).attempts, 1);
});

test('poll loop runs jobs with bounded concurrency', async () => {
  let active = 0;
  let peak = 0;
  const processFile = async () => { active++; peak = Math.max(peak, active); await sleep(100); active--; return { totalRecords: 0 }; };
  const { queue, consumer } = build({ processFile });
  const seeded = await Promise.all([seed(), seed(), seed(), seed(), seed()]);
  queue.pending.push(...seeded.map((s) => s.message));
  consumer.start();
  while (queue.deleted.length < 5) await sleep(25);
  await consumer.stop();
  assert.equal(peak, 2);
});

test('does not poll while Redis is not ready', async () => {
  let polls = 0;
  const queue = fakeQueue();
  const receive = queue.receive;
  queue.receive = (a) => { polls++; return receive(a); };
  const { consumer } = build({ queue, isReady: () => false });
  consumer.start();
  await sleep(200);
  await consumer.stop();
  assert.equal(polls, 0);
});

test('graceful shutdown: in-flight job finishes and is acknowledged', async () => {
  const { queue, consumer } = build({ processFile: async () => { await sleep(300); return { totalRecords: 1 }; } });
  const { jobId, message } = await seed();
  queue.pending.push(message);
  consumer.start();
  while ((await env.jobs.get(jobId)).status !== 'processing') await sleep(20);
  await consumer.stop();
  assert.equal((await env.jobs.get(jobId)).status, 'completed');
  assert.deepEqual(queue.deleted, [message.ReceiptHandle]);
});

test('shutdown timeout: work is released safely, message kept for redelivery', async () => {
  const processFile = (body, type, signal) => new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
  const { queue, consumer } = build({ processFile }, { shutdownTimeoutMs: 150 });
  const { jobId, message } = await seed();
  queue.pending.push(message);
  consumer.start();
  while ((await env.jobs.get(jobId)).status !== 'processing') await sleep(20);
  await consumer.stop();
  assert.deepEqual(queue.deleted, []);
  assert.equal(await env.redis.exists(keys.lock(jobId)), 0);
  assert.equal((await env.jobs.get(jobId)).status, 'processing', 'state untouched so the next delivery can re-claim it');
  const next = build();
  assert.equal(await next.consumer.handleMessage(message), 'completed');
});
