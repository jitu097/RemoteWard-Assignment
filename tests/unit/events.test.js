import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildFileUploadedEvent, validateEvent } from '../../src/utils/events.js';
import { PermanentError } from '../../src/utils/errors.js';

const valid = () =>
  buildFileUploadedEvent({
    jobId: '3f2b8c1e-5a4d-4e6f-8a9b-1c2d3e4f5a6b',
    bucket: 'b',
    key: 'uploads/2026/11/24/3f2b8c1e-5a4d-4e6f-8a9b-1c2d3e4f5a6b.csv',
    originalName: 'customers.csv',
    contentType: 'text/csv',
    size: 10,
  });

test('built event round-trips through validation and carries no file contents', () => {
  const e = valid();
  assert.equal(e.eventType, 'FILE_UPLOADED');
  assert.equal(e.eventVersion, 1);
  assert.deepEqual(validateEvent(JSON.parse(JSON.stringify(e))), e);
});

test('rejects unsupported version as permanent', () => {
  assert.throws(() => validateEvent({ ...valid(), eventVersion: 2 }), (e) => e instanceof PermanentError && /eventVersion/.test(e.message));
});

for (const [name, mutate] of [
  ['non-object', () => 'nope'],
  ['wrong type', (e) => ({ ...e, eventType: 'OTHER' })],
  ['bad jobId', (e) => ({ ...e, jobId: '../etc/passwd' })],
  ['key outside uploads/', (e) => ({ ...e, file: { ...e.file, key: 'secrets/x.csv' } })],
  ['bad content type', (e) => ({ ...e, file: { ...e.file, contentType: 'image/png' } })],
  ['negative size', (e) => ({ ...e, file: { ...e.file, size: -1 } })],
  ['missing file', (e) => ({ ...e, file: undefined })],
  ['bad timestamp', (e) => ({ ...e, uploadedAt: 'yesterday' })],
]) {
  test(`rejects malformed event: ${name}`, () => {
    assert.throws(() => validateEvent(mutate(valid())), PermanentError);
  });
}
