import { PermanentError } from '../utils/errors.js';
import { processCsv } from './parsers/csv.js';
import { processJson } from './parsers/json.js';

/** Turns a readable stream of a supported content type into the deterministic statistics result. */
export function processStream(source, contentType, signal) {
  if (contentType === 'text/csv') return processCsv(source, signal);
  if (contentType === 'application/json') return processJson(source, signal);
  throw new PermanentError(`Unsupported content type ${contentType}`);
}
