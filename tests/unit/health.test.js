import { test } from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';
import { createApp } from '../../src/api/app.js';
import { createLogger } from '../../src/utils/logger.js';

const app = createApp({ config: { trustProxy: 0 }, logger: createLogger({ service: 'test', level: 'error', write: () => {} }) });

test('GET /health', async () => {
  const res = await request(app).get('/health');
  assert.equal(res.status, 200);
  assert.deepEqual(res.body, { status: 'ok' });
});

test('unknown route returns consistent error body', async () => {
  const res = await request(app).get('/nope');
  assert.equal(res.status, 404);
  assert.equal(res.body.error.code, 'NOT_FOUND');
});
