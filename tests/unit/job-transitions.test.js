import { test } from 'node:test';
import assert from 'node:assert/strict';
import { canTransition } from '../../src/services/job.service.js';

test('valid lifecycle transitions', () => {
  assert.ok(canTransition('queued', 'processing'));
  assert.ok(canTransition('processing', 'completed'));
  assert.ok(canTransition('processing', 'failed'));
  assert.ok(canTransition('failed', 'processing')); // retry after redelivery
  assert.ok(canTransition('processing', 'processing')); // re-claim after a crashed worker
  assert.ok(canTransition('queued', 'enqueue_failed'));
  assert.ok(canTransition('enqueue_failed', 'queued')); // operator recovery
});

test('completed is terminal', () => {
  for (const to of ['queued', 'processing', 'failed', 'enqueue_failed', 'completed']) {
    assert.equal(canTransition('completed', to), false, `completed -> ${to}`);
  }
});

test('invalid transitions are refused', () => {
  assert.equal(canTransition('queued', 'completed'), false);
  assert.equal(canTransition('enqueue_failed', 'processing'), false);
  assert.equal(canTransition('failed', 'completed'), false);
  assert.equal(canTransition('queued', 'nonsense'), false);
});
