import { streamArray } from 'stream-json/streamers/stream-array.js';
import { PermanentError } from '../../utils/errors.js';
import { createStats } from './stats.js';
import { parsedRecords } from './stream.js';

/**
 * Streams a top-level JSON array of objects, assembling one element at a time.
 * Fatal (no partial results): invalid JSON, a non-array root, or an element that is not a plain object.
 * Nested objects/arrays count as non-empty, non-numeric values; they are not flattened.
 */
export async function processJson(source, signal) {
  const stats = createStats({ numericStrings: false });
  const parser = streamArray.withParserAsStream();
  for await (const { key, value } of parsedRecords(source, parser, signal)) {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) {
      throw new PermanentError(`Array element ${key} is not an object`);
    }
    stats.add(value);
  }
  return { fileType: 'json', ...stats.result() };
}
