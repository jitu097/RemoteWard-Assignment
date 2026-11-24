// Rate limiting against a real Redis: shared counters across two app instances, 429s, headers, proxies.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { createApp } from '../../src/api/app.js';
import { createRateLimiters } from '../../src/api/middleware/rate-limit.js';
import { setupRedis } from './helpers.js';

let env;
let tag;
before(async () => {
  env = await setupRedis({ READ_RATE_LIMIT_MAX: '3', UPLOAD_RATE_LIMIT_MAX: '2', TRUST_PROXY: '1' });
});
after(async () => {
  const keys = await env.redis.keys(`rl:*${tag}*`);
  if (keys.length) await env.redis.del(keys);
  await env.close();
});

// Each test uses a unique client IP (via X-Forwarded-For) so counters never collide between tests or runs.
const ip = () => `10.${Math.floor(Math.random() * 250)}.${Math.floor(Math.random() * 250)}.${Math.floor(Math.random() * 250)}`;

function instance() {
  const noJobs = { get: async () => null, getResult: async () => null };
  return createApp({
    config: env.config,
    logger: env.logger,
    jobs: noJobs,
    s3: {},
    queue: {},
    limiters: createRateLimiters({ redis: env.redis, config: env.config, logger: env.logger }),
  });
}

test('read limit: allowed up to the max, then 429 with standard headers and error body', async () => {
  const app = instance();
  const client = ip();
  tag = client;
  const url = `/api/v1/jobs/${randomUUID()}`;
  for (let i = 0; i < 3; i++) {
    const res = await request(app).get(url).set('X-Forwarded-For', client);
    assert.equal(res.status, 404); // reaches the controller
    assert.ok(res.headers['ratelimit'], 'RateLimit header present');
    assert.ok(res.headers['ratelimit-policy'], 'RateLimit-Policy header present');
  }
  const limited = await request(app).get(url).set('X-Forwarded-For', client);
  assert.equal(limited.status, 429);
  assert.equal(limited.body.error.code, 'RATE_LIMITED');
  assert.ok(limited.headers['retry-after']);
});

test('limits are shared across API instances via Redis', async () => {
  const [a, b] = [instance(), instance()];
  const client = ip();
  const url = `/api/v1/jobs/${randomUUID()}`;
  assert.equal((await request(a).get(url).set('X-Forwarded-For', client)).status, 404);
  assert.equal((await request(b).get(url).set('X-Forwarded-For', client)).status, 404);
  assert.equal((await request(a).get(url).set('X-Forwarded-For', client)).status, 404);
  assert.equal((await request(b).get(url).set('X-Forwarded-For', client)).status, 429, 'instance B sees A\'s requests');
});

test('different client IPs have independent counters (trusted proxy header is honoured)', async () => {
  const app = instance();
  const url = `/api/v1/jobs/${randomUUID()}`;
  const [one, two] = [ip(), ip()];
  for (let i = 0; i < 3; i++) await request(app).get(url).set('X-Forwarded-For', one);
  assert.equal((await request(app).get(url).set('X-Forwarded-For', one)).status, 429);
  assert.equal((await request(app).get(url).set('X-Forwarded-For', two)).status, 404);
});

test('upload limit is separate from the read limit and rejects before any upload work', async () => {
  const app = instance();
  const client = ip();
  const post = () => request(app).post('/api/v1/uploads').set('X-Forwarded-For', client).field('x', 'y');
  assert.equal((await post()).status, 400); // NO_FILE: passed the limiter
  assert.equal((await post()).status, 400);
  assert.equal((await post()).status, 429);
  assert.equal((await request(app).get(`/api/v1/jobs/${randomUUID()}`).set('X-Forwarded-For', client)).status, 404);
});

test('/health is never rate limited', async () => {
  const app = instance();
  const client = ip();
  for (let i = 0; i < 6; i++) assert.equal((await request(app).get('/health').set('X-Forwarded-For', client)).status, 200);
});

test('counters live in Redis under the rl: prefix with a TTL', async () => {
  const app = instance();
  const client = ip();
  await request(app).get(`/api/v1/jobs/${randomUUID()}`).set('X-Forwarded-For', client);
  const keys = await env.redis.keys(`rl:read:${client}*`);
  assert.equal(keys.length, 1);
  assert.ok((await env.redis.pttl(keys[0])) > 0);
});

test('Redis outage fails open instead of failing requests', async () => {
  const broken = { call: async () => { throw new Error('Redis down'); } };
  const app = createApp({
    config: env.config,
    logger: env.logger,
    jobs: { get: async () => null },
    s3: {},
    queue: {},
    limiters: createRateLimiters({ redis: broken, config: env.config, logger: env.logger }),
  });
  const res = await request(app).get(`/api/v1/jobs/${randomUUID()}`).set('X-Forwarded-For', ip());
  assert.equal(res.status, 404);
});
