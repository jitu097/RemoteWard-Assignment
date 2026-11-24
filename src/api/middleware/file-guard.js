import { Transform } from 'node:stream';
import path from 'node:path';
import { errors } from '../../utils/errors.js';

const TYPES = {
  '.csv': 'text/csv',
  '.json': 'application/json',
};

// Declared types we tolerate. The client header is only a hint: content sniffing decides.
const ACCEPTED_DECLARED = new Set([
  'text/csv', 'application/csv', 'application/vnd.ms-excel', 'text/plain',
  'application/json', 'text/json', 'application/octet-stream',
]);

export function resolveType(fileName, declaredMime) {
  const ext = path.extname(fileName ?? '').toLowerCase();
  const contentType = TYPES[ext];
  if (!contentType) throw errors.unsupported('Only .csv and .json files are accepted');
  if (declaredMime && !ACCEPTED_DECLARED.has(declaredMime.toLowerCase())) {
    throw errors.unsupported(`Declared content type ${declaredMime} is not accepted`);
  }
  return { ext, contentType };
}

/** Strips directories and control characters from the client-supplied name; metadata only, never used in S3 keys. */
export function cleanFileName(name) {
  // eslint-disable-next-line no-control-regex
  return path.basename(String(name).replace(/\\/g, '/')).replace(/[\u0000-\u001f\u007f]/g, '').slice(0, 255);
}

/** Returns an error message if the first chunk is not plausibly the claimed type, else null. */
export function sniffFile(contentType, chunk) {
  let bytes = chunk;
  if (bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) bytes = bytes.subarray(3); // UTF-8 BOM
  if ((bytes[0] === 0xff && bytes[1] === 0xfe) || (bytes[0] === 0xfe && bytes[1] === 0xff)) {
    return 'UTF-16 encoded files are not supported; use UTF-8';
  }
  const head = bytes.subarray(0, 1024);
  for (const b of head) {
    if (b < 0x20 && b !== 0x09 && b !== 0x0a && b !== 0x0d) return 'File content is binary, not text';
  }
  if (contentType === 'application/json') {
    const first = head.toString('utf8').trimStart()[0];
    if (first !== '[') return 'JSON files must contain an array of objects';
  }
  return null;
}

/**
 * Transform that enforces the size limit and sniffs the first chunk while data streams through.
 * Nothing is buffered beyond the chunk currently in flight.
 */
export function createFileGuard({ contentType, maxBytes }) {
  let total = 0;
  let first = true;
  return new Transform({
    transform(chunk, _enc, cb) {
      if (first) {
        first = false;
        const problem = sniffFile(contentType, chunk);
        if (problem) return cb(errors.unsupported(problem));
      }
      total += chunk.length;
      if (total > maxBytes) return cb(errors.tooLarge(maxBytes));
      cb(null, chunk);
    },
    flush(cb) {
      if (total === 0) return cb(errors.badRequest('EMPTY_FILE', 'File is empty'));
      cb();
    },
  });
}
