// Job state and locking against a real Redis (Lua scripts included).
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { setupRedis } from './helpers.js';
import { keys } from '../../src/services/job.service.js';

let env;
const file = { bucket: 'b', key: 'uploads/2026/11/24/x.csv', originalName: 'x.csv', contentType: 'text/csv', size: 3 };

before(async () => {
  env = await setupRedis({ JOB_LOCK_TTL_SECONDS: '5', SQS_VISIBILITY_TIMEOUT_SECONDS: '30' });
});
after(() => env.close());

test('create is exclusive and sets TTLs', async () => {
  const id = randomUUID();
  assert.equal((await env.jobs.create(id, file)).status, 'queued');
  assert.equal(await env.jobs.create(id, file), null);
  const ttl = await env.redis.ttl(keys.status(id));
  assert.ok(ttl > 0 && ttl <= 86400);
});

test('happy path: queued -> processing -> completed stores result atomically', async () => {
  const id = randomUUID();
  await env.jobs.create(id, file);
  const started = await env.jobs.markProcessing(id);
  assert.equal(started.record.status, 'processing');
  assert.equal(started.record.attempts, 1);
  assert.equal(await env.jobs.getResult(id), null);
  const done = await env.jobs.complete(id, { totalRecords: 7 });
  assert.equal(done.ok, true);
  assert.equal((await env.jobs.get(id)).status, 'completed');
  assert.deepEqual(await env.jobs.getResult(id), { totalRecords: 7 });
  assert.ok((await env.redis.ttl(keys.result(id))) > 0);
});

test('invalid transitions are refused and leave the record untouched', async () => {
  const id = randomUUID();
  await env.jobs.create(id, file);
  const res = await env.jobs.complete(id, { x: 1 }); // queued -> completed is not allowed
  assert.deepEqual([res.ok, res.current], [false, 'queued']);
  assert.equal(await env.jobs.getResult(id), null, 'result must not be written by a refused transition');
  assert.deepEqual(await env.jobs.markProcessing(randomUUID()), { ok: false, current: 'missing' });
});

test('completed jobs cannot be overwritten by stale duplicates', async () => {
  const id = randomUUID();
  await env.jobs.create(id, file);
  await env.jobs.markProcessing(id);
  await env.jobs.complete(id, { totalRecords: 1 });
  assert.equal((await env.jobs.markProcessing(id)).ok, false);
  assert.equal((await env.jobs.fail(id, { code: 'X' })).ok, false);
  assert.equal((await env.jobs.complete(id, { totalRecords: 999 })).ok, false);
  assert.deepEqual(await env.jobs.getResult(id), { totalRecords: 1 });
  assert.equal((await env.jobs.get(id)).status, 'completed');
});

test('failed jobs can be retried; attempts accumulate', async () => {
  const id = randomUUID();
  await env.jobs.create(id, file);
  await env.jobs.markProcessing(id);
  await env.jobs.fail(id, { code: 'TRANSIENT', message: 'boom' });
  const failed = await env.jobs.get(id);
  assert.equal(failed.status, 'failed');
  assert.equal(failed.error.message, 'boom');
  const again = await env.jobs.markProcessing(id);
  assert.equal(again.record.attempts, 2);
});

test('enqueue_failed can be re-queued, and only from that state', async () => {
  const id = randomUUID();
  await env.jobs.create(id, file);
  assert.equal((await env.jobs.markEnqueueFailed(id, { code: 'ENQUEUE_FAILED' })).ok, true);
  assert.equal((await env.jobs.markProcessing(id)).ok, false);
  assert.equal((await env.jobs.requeue(id)).ok, true);
  assert.equal((await env.jobs.requeue(id)).ok, false);
});

test('concurrent markProcessing + complete race resolves to a single winner', async () => {
  const id = randomUUID();
  await env.jobs.create(id, file);
  await env.jobs.markProcessing(id);
  const results = await Promise.all([env.jobs.complete(id, { w: 'a' }), env.jobs.complete(id, { w: 'b' })]);
  assert.equal(results.filter((r) => r.ok).length, 1);
});

test('result expiry: status stays but result is gone', async () => {
  const short = await setupRedis({ JOB_RESULT_TTL_SECONDS: '1' });
  try {
    const id = randomUUID();
    await short.jobs.create(id, file);
    await short.jobs.markProcessing(id);
    await short.jobs.complete(id, { a: 1 });
    await sleep(1500);
    assert.equal((await short.jobs.get(id)).status, 'completed');
    assert.equal(await short.jobs.getResult(id), null);
  } finally {
    await short.close();
  }
});

test('lock: exclusive, token-owned renew/release, expires', async () => {
  const id = randomUUID();
  assert.equal(await env.jobs.acquireLock(id, 'A'), true);
  assert.equal(await env.jobs.acquireLock(id, 'B'), false);
  assert.equal(await env.jobs.renewLock(id, 'B'), false);
  assert.equal(await env.jobs.releaseLock(id, 'B'), false, 'a non-owner must not release');
  assert.equal(await env.jobs.renewLock(id, 'A'), true);
  assert.equal(await env.jobs.releaseLock(id, 'A'), true);
  assert.equal(await env.jobs.acquireLock(id, 'B'), true);
  assert.ok((await env.redis.ttl(keys.lock(id))) <= 5);
});
