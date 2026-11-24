// Real S3 API + real SQS API (local emulator by default) + real Redis.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import request from 'supertest';
import { createApp } from '../../src/api/app.js';
import { setupEnv } from './helpers.js';

let env;
let app;

before(async () => {
  env = await setupEnv();
  app = createApp(env);
});
after(() => env.close());

test('upload streams to S3, publishes to SQS, records queued job in Redis', async () => {
  const res = await request(app).post('/api/v1/uploads').attach('file', 'tests/fixtures/customers.csv', { contentType: 'text/csv' });
  assert.equal(res.status, 202, JSON.stringify(res.body));
  const { jobId } = res.body.data;

  const job = await env.jobs.get(jobId);
  assert.equal(job.status, 'queued');

  const head = await env.rawS3.send(new HeadObjectCommand({ Bucket: env.config.s3.bucket, Key: job.file.key }));
  assert.equal(head.ContentType, 'text/csv');
  assert.equal(head.ServerSideEncryption, 'AES256');
  const obj = await env.rawS3.send(new GetObjectCommand({ Bucket: env.config.s3.bucket, Key: job.file.key }));
  assert.equal(await obj.Body.transformToString(), readFileSync('tests/fixtures/customers.csv', 'utf8'));

  const [msg] = await env.queue.receive({ max: 1, waitSeconds: 1 });
  const event = JSON.parse(msg.Body);
  assert.equal(event.jobId, jobId);
  assert.equal(event.file.key, job.file.key);
  assert.ok(!msg.Body.includes('Alice'), 'file contents must not be in the event');
  await env.queue.delete(msg.ReceiptHandle);

  const status = await request(app).get(`/api/v1/jobs/${jobId}`);
  assert.equal(status.body.data.status, 'queued');
});

test('multipart path: a 12 MiB upload streams to S3 intact', async () => {
  const header = 'id,value\n';
  const rows = Buffer.alloc(12 * 1024 * 1024, '1,2\n');
  const body = Buffer.concat([Buffer.from(header), rows]);
  const big = createApp({ ...env, config: { ...env.config, upload: { maxBytes: 20 * 1024 * 1024 } } });
  const res = await request(big).post('/api/v1/uploads').attach('file', body, { filename: 'big.csv', contentType: 'text/csv' });
  assert.equal(res.status, 202, JSON.stringify(res.body));
  const job = await env.jobs.get(res.body.data.jobId);
  const head = await env.rawS3.send(new HeadObjectCommand({ Bucket: env.config.s3.bucket, Key: job.file.key }));
  assert.equal(head.ContentLength, body.length);
  const [msg] = await env.queue.receive({ max: 1, waitSeconds: 1 });
  await env.queue.delete(msg.ReceiptHandle);
});

test('queue failure after real S3 upload leaves object + enqueue_failed job', async () => {
  const brokenQueue = { publish: async () => { throw new Error('simulated SQS outage'); } };
  const broken = createApp({ ...env, queue: brokenQueue });
  const res = await request(broken).post('/api/v1/uploads').attach('file', 'tests/fixtures/orders.json', { contentType: 'application/json' });
  assert.equal(res.status, 503);
  const jobId = /Job ([0-9a-f-]{36})/.exec(res.body.error.message)[1];
  const job = await env.jobs.get(jobId);
  assert.equal(job.status, 'enqueue_failed');
  await env.rawS3.send(new HeadObjectCommand({ Bucket: env.config.s3.bucket, Key: job.file.key })); // still there
});
