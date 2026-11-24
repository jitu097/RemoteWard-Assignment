import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig } from '../../src/config/env.js';

const base = {
  AWS_REGION: 'us-east-1',
  S3_BUCKET: 'b',
  SQS_QUEUE_URL: 'http://q',
  SQS_DLQ_URL: 'http://dlq',
  REDIS_URL: 'redis://localhost:6379',
};

test('applies defaults and parses numbers', () => {
  const c = loadConfig({ ...base, PORT: '4000' });
  assert.equal(c.port, 4000);
  assert.equal(c.upload.maxBytes, 10 * 1024 * 1024);
  assert.equal(c.rateLimit.upload.max, 30);
});

test('reports every problem at once', () => {
  assert.throws(() => loadConfig({ PORT: 'abc' }), (e) => /PORT/.test(e.message) && /S3_BUCKET/.test(e.message) && /REDIS_URL/.test(e.message));
});

test('lock TTL must be shorter than visibility timeout', () => {
  assert.throws(() => loadConfig({ ...base, JOB_LOCK_TTL_SECONDS: '200' }), /JOB_LOCK_TTL_SECONDS/);
});
