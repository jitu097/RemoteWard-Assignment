import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { cleanFileName, createFileGuard, resolveType, sniffFile } from '../../src/api/middleware/file-guard.js';

test('resolveType maps extension to canonical type and ignores case', () => {
  assert.deepEqual(resolveType('A.CSV', 'text/csv'), { ext: '.csv', contentType: 'text/csv' });
  assert.equal(resolveType('a.json', undefined).contentType, 'application/json');
});

test('resolveType rejects unknown extension and contradictory declared type', () => {
  assert.throws(() => resolveType('a.exe', 'text/csv'), { code: 'UNSUPPORTED_FILE_TYPE' });
  assert.throws(() => resolveType('a.csv', 'image/png'), { code: 'UNSUPPORTED_FILE_TYPE' });
});

test('sniffFile accepts text CSV (with BOM) and JSON arrays', () => {
  assert.equal(sniffFile('text/csv', Buffer.from('﻿id,name\n1,a\n')), null);
  assert.equal(sniffFile('application/json', Buffer.from('  \n[{"a":1}]')), null);
});

test('sniffFile rejects binary, UTF-16 and non-array JSON', () => {
  assert.match(sniffFile('text/csv', Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0])), /binary/);
  assert.match(sniffFile('text/csv', Buffer.from([0xff, 0xfe, 0x69, 0x00])), /UTF-16/);
  assert.match(sniffFile('application/json', Buffer.from('{"a":1}')), /array/);
  assert.match(sniffFile('application/json', Buffer.from('id,name\n1,a')), /array/);
});

test('cleanFileName strips paths and control characters', () => {
  assert.equal(cleanFileName('..\\..\\evil/../a.csv'), 'a.csv');
  assert.equal(cleanFileName('a\u0000b.csv'), 'ab.csv');
});

const run = (guard, chunks) => pipeline(Readable.from(chunks), guard, async (src) => { for await (const chunk of src) void chunk; });

test('guard enforces the size limit while streaming', async () => {
  const guard = createFileGuard({ contentType: 'text/csv', maxBytes: 10 });
  await assert.rejects(run(guard, [Buffer.from('a,b\n1,2\n'), Buffer.from('3,4\n')]), { code: 'FILE_TOO_LARGE' });
});

test('guard rejects empty files', async () => {
  const guard = createFileGuard({ contentType: 'text/csv', maxBytes: 10 });
  await assert.rejects(run(guard, []), { code: 'EMPTY_FILE' });
});
