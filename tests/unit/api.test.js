// API tests with in-memory fakes for S3, SQS and Redis. These are MOCKED tests, not AWS verification.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { createApp } from '../../src/api/app.js';
import { createLogger } from '../../src/utils/logger.js';

const config = { trustProxy: 0, upload: { maxBytes: 1024 }, s3: { bucket: 'test-bucket' } };
const logger = createLogger({ service: 'test', level: 'error', write: () => {} });

function fakes({ s3Fail, queueFail, createFail } = {}) {
  const state = { objects: new Map(), published: [], jobs: new Map(), results: new Map() };
  const s3 = {
    async upload({ key, body }) {
      const chunks = [];
      for await (const c of body) chunks.push(c); // consuming the stream surfaces guard errors
      if (s3Fail) throw new Error('S3 down');
      state.objects.set(key, Buffer.concat(chunks));
    },
  };
  const queue = {
    async publish(event) {
      if (queueFail) throw new Error('SQS down');
      state.published.push(event);
    },
  };
  const jobs = {
    async create(jobId, file) {
      if (createFail) throw new Error('Redis down');
      const rec = { jobId, status: 'queued', file, attempts: 0, createdAt: 't', updatedAt: 't' };
      state.jobs.set(jobId, rec);
      return rec;
    },
    async get(id) { return state.jobs.get(id) ?? null; },
    async getResult(id) { return state.results.get(id) ?? null; },
    async markEnqueueFailed(id, error) { Object.assign(state.jobs.get(id), { status: 'enqueue_failed', error }); },
  };
  const pass = (req, res, next) => next(); // rate limiting is covered by the Redis-backed integration test
  return { state, app: createApp({ config, logger, s3, queue, jobs, limiters: { upload: pass, read: pass } }) };
}

const upload = (app, content, filename, type) =>
  request(app).post('/api/v1/uploads').attach('file', Buffer.from(content), { filename, contentType: type });

test('valid CSV upload: stored, published after storage, 202 with job info', async () => {
  const { app, state } = fakes();
  const res = await upload(app, 'id,name\n1,a\n', 'customers.csv', 'text/csv');
  assert.equal(res.status, 202);
  assert.equal(res.body.success, true);
  assert.equal(res.body.message, 'File accepted for processing');
  const { jobId, status, fileName, statusUrl } = res.body.data;
  assert.deepEqual([status, fileName, statusUrl], ['queued', 'customers.csv', `/api/v1/jobs/${jobId}`]);
  assert.equal(state.objects.size, 1);
  const [key] = state.objects.keys();
  assert.match(key, new RegExp(`^uploads/\\d{4}/\\d{2}/\\d{2}/${jobId}\\.csv$`));
  assert.equal(state.published.length, 1);
  assert.equal(state.published[0].file.key, key);
  assert.equal(state.published[0].file.size, 12);
});

test('valid JSON upload', async () => {
  const { app } = fakes();
  const res = await upload(app, '[{"a":1}]', 'orders.json', 'application/json');
  assert.equal(res.status, 202);
});

test('rejects wrong extension, binary content, non-array JSON, empty file', async () => {
  const { app, state } = fakes();
  assert.equal((await upload(app, 'x', 'a.exe', 'text/csv')).status, 415);
  assert.equal((await upload(app, Buffer.from([0, 1, 2, 3]), 'a.csv', 'text/csv')).status, 415);
  assert.equal((await upload(app, '{"a":1}', 'a.json', 'application/json')).status, 415);
  assert.equal((await upload(app, '', 'a.csv', 'text/csv')).status, 400);
  assert.equal(state.published.length, 0);
});

test('rejects request without a file part', async () => {
  const { app } = fakes();
  const res = await request(app).post('/api/v1/uploads').field('x', 'y');
  assert.equal(res.status, 400);
  assert.equal(res.body.error.code, 'NO_FILE');
});

test('rejects non-multipart body', async () => {
  const { app } = fakes();
  const res = await request(app).post('/api/v1/uploads').send({ a: 1 });
  assert.equal(res.status, 415);
});

test('oversized file: 413 and nothing published', async () => {
  const { app, state } = fakes();
  const res = await upload(app, 'a'.repeat(5000), 'big.csv', 'text/csv');
  assert.equal(res.status, 413);
  assert.equal(res.body.error.code, 'FILE_TOO_LARGE');
  assert.equal(state.published.length, 0);
});

test('S3 failure: 503, no event, no job', async () => {
  const { app, state } = fakes({ s3Fail: true });
  const res = await upload(app, 'a,b\n1,2\n', 'a.csv', 'text/csv');
  assert.equal(res.status, 503);
  assert.equal(res.body.error.code, 'STORAGE_UNAVAILABLE');
  assert.equal(state.published.length, 0);
  assert.equal(state.jobs.size, 0);
});

test('queue failure: not reported as queued, job kept as enqueue_failed', async () => {
  const { app, state } = fakes({ queueFail: true });
  const res = await upload(app, 'a,b\n1,2\n', 'a.csv', 'text/csv');
  assert.equal(res.status, 503);
  assert.equal(res.body.error.code, 'ENQUEUE_FAILED');
  assert.match(res.body.error.message, /Job [0-9a-f-]{36}/);
  assert.equal(state.objects.size, 1, 'uploaded file is kept');
  const [job] = state.jobs.values();
  assert.equal(job.status, 'enqueue_failed');
  assert.equal(state.published.length, 0);
});

test('Redis failure after S3 upload: 503 and no event published', async () => {
  const { app, state } = fakes({ createFail: true });
  const res = await upload(app, 'a,b\n1,2\n', 'a.csv', 'text/csv');
  assert.equal(res.status, 503);
  assert.equal(res.body.error.code, 'JOB_STORE_UNAVAILABLE');
  assert.equal(state.published.length, 0);
});

test('job retrieval, missing job, invalid id', async () => {
  const { app, state } = fakes();
  const id = randomUUID();
  state.jobs.set(id, { jobId: id, status: 'processing', file: { originalName: 'a.csv' }, attempts: 1, createdAt: 't', updatedAt: 't' });
  const ok = await request(app).get(`/api/v1/jobs/${id}`);
  assert.equal(ok.status, 200);
  assert.equal(ok.body.data.status, 'processing');
  assert.equal((await request(app).get(`/api/v1/jobs/${randomUUID()}`)).status, 404);
  assert.equal((await request(app).get('/api/v1/jobs/not-a-uuid')).status, 400);
});

test('result: not ready (409), expired (410), available (200), missing job (404)', async () => {
  const { app, state } = fakes();
  const id = randomUUID();
  state.jobs.set(id, { jobId: id, status: 'queued', file: { originalName: 'a.csv' }, attempts: 0, createdAt: 't', updatedAt: 't' });
  assert.equal((await request(app).get(`/api/v1/jobs/${id}/result`)).status, 409);

  state.jobs.get(id).status = 'completed';
  const expired = await request(app).get(`/api/v1/jobs/${id}/result`);
  assert.equal(expired.status, 410);
  assert.equal(expired.body.error.code, 'RESULT_EXPIRED');

  state.results.set(id, { totalRecords: 3 });
  const ok = await request(app).get(`/api/v1/jobs/${id}/result`);
  assert.equal(ok.status, 200);
  assert.equal(ok.body.data.result.totalRecords, 3);

  assert.equal((await request(app).get(`/api/v1/jobs/${randomUUID()}/result`)).status, 404);
});
