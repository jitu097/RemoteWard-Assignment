import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { createReadStream } from 'node:fs';
import { processStream } from '../../src/worker/processor.js';
import { PermanentError } from '../../src/utils/errors.js';

const csv = (text) => processStream(Readable.from([Buffer.from(text)]), 'text/csv');
const json = (text) => processStream(Readable.from([Buffer.from(text)]), 'application/json');

test('CSV fixture: counts, columns, empties, numeric stats', async () => {
  const r = await processStream(createReadStream('tests/fixtures/customers.csv'), 'text/csv');
  assert.equal(r.fileType, 'csv');
  assert.equal(r.totalRecords, 4);
  assert.equal(r.totalColumns, 5);
  assert.deepEqual(r.columns, ['id', 'name', 'age', 'city', 'balance']);
  assert.deepEqual(r.emptyValues, { id: 0, name: 0, age: 1, city: 1, balance: 0 });
  assert.deepEqual(r.numericStats.age, { count: 3, min: 29, max: 41, sum: 104, mean: 34.666667 });
  assert.deepEqual(r.numericStats.balance, { count: 4, min: -50.25, max: 1200.5, sum: 1450.25, mean: 362.5625 });
  assert.equal(r.numericStats.name, undefined);
});

test('CSV: quoted fields, embedded commas/newlines, BOM, blank lines, header only', async () => {
  const r = await csv('﻿a,b\n"x,1","line1\nline2"\n\n"2",y\n');
  assert.equal(r.totalRecords, 2);
  assert.deepEqual(r.columns, ['a', 'b']);
  assert.deepEqual((await csv('a,b\n')).totalRecords, 0);
  assert.deepEqual((await csv('a,b\n')).columns, ['a', 'b']);
});

test('CSV: a column with any non-numeric value gets no numeric stats; whitespace counts as empty', async () => {
  const r = await csv('n,m\n1,  \n2,x\n3,4\n');
  assert.equal(r.numericStats.n.count, 3);
  assert.equal(r.numericStats.m, undefined);
  assert.equal(r.emptyValues.m, 1);
});

test('CSV: output is deterministic', async () => {
  const text = 'a,b\n1,2\n3,\n';
  assert.deepEqual(await csv(text), await csv(text));
});

for (const [name, text] of [
  ['ragged row', 'a,b\n1,2\n3\n'],
  ['duplicate header', 'a,a\n1,2\n'],
  ['blank header name', 'a,\n1,2\n'],
  ['unterminated quote', 'a,b\n"1,2\n'],
]) {
  test(`CSV malformed (${name}) is a PermanentError`, async () => {
    await assert.rejects(csv(text), PermanentError);
  });
}

test('JSON fixture: counts, keys, empties, numeric stats', async () => {
  const r = await processStream(createReadStream('tests/fixtures/orders.json'), 'application/json');
  assert.equal(r.fileType, 'json');
  assert.equal(r.totalRecords, 3);
  assert.deepEqual(r.columns, ['orderId', 'customer', 'total', 'coupon']);
  assert.deepEqual(r.emptyValues, { orderId: 0, customer: 1, total: 0, coupon: 2 }); // null, "", and missing key
  assert.deepEqual(r.numericStats.total, { count: 3, min: 5.25, max: 99.5, sum: 124.75, mean: 41.583333 });
  assert.equal(r.numericStats.customer, undefined);
});

test('JSON: empty array, nested values, numeric strings are not coerced', async () => {
  assert.equal((await json('[]')).totalRecords, 0);
  const r = await json('[{"a":{"x":1},"n":"5"},{"a":[1],"n":"6"}]');
  assert.equal(r.emptyValues.a, 0);
  assert.equal(r.numericStats.a, undefined);
  assert.equal(r.numericStats.n, undefined);
});

for (const [name, text] of [
  ['invalid syntax', '[{"a":1},'],
  ['object root', '{"a":1}'],
  ['non-object element', '[1,2]'],
  ['array element', '[[1]]'],
]) {
  test(`JSON malformed (${name}) is a PermanentError`, async () => {
    await assert.rejects(json(text), PermanentError);
  });
}

test('source stream failure is NOT classified as permanent (transient)', async () => {
  const source = new Readable({ read() {} });
  const p = processStream(source, 'text/csv');
  source.push('a,b\n1,2\n');
  setImmediate(() => source.destroy(Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' })));
  await assert.rejects(p, (e) => !(e instanceof PermanentError) && e.code === 'ECONNRESET');
});

test('streams large input without holding it in memory', async () => {
  const rows = 200_000;
  async function* gen() {
    yield 'id,v\n';
    for (let i = 0; i < rows; i++) yield `${i},${i % 7}\n`;
  }
  const before = process.memoryUsage().heapUsed;
  const r = await processStream(Readable.from(gen()), 'text/csv');
  assert.equal(r.totalRecords, rows);
  assert.equal(r.numericStats.v.max, 6);
  assert.ok(process.memoryUsage().heapUsed - before < 100 * 1024 * 1024);
});

test('unsupported content type is permanent', () => {
  assert.throws(() => processStream(Readable.from([]), 'image/png'), PermanentError);
});
