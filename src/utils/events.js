import { PermanentError } from './errors.js';

export const EVENT_TYPE = 'FILE_UPLOADED';
export const EVENT_VERSION = 1;
export const CONTENT_TYPES = ['text/csv', 'application/json'];

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const isJobId = (v) => typeof v === 'string' && UUID.test(v);

/** Builds the versioned FILE_UPLOADED event. Metadata only: file contents never travel through SQS. */
export function buildFileUploadedEvent({ jobId, bucket, key, originalName, contentType, size, uploadedAt = new Date() }) {
  return {
    eventType: EVENT_TYPE,
    eventVersion: EVENT_VERSION,
    jobId,
    file: { bucket, key, originalName, contentType, size },
    uploadedAt: uploadedAt.toISOString(),
  };
}

/**
 * Validates an untrusted event (parsed SQS body). Returns the event or throws PermanentError,
 * because a malformed or unsupported event can never succeed on retry.
 */
export function validateEvent(event) {
  const problems = [];
  if (event === null || typeof event !== 'object' || Array.isArray(event)) {
    throw new PermanentError('Event must be a JSON object');
  }
  if (event.eventType !== EVENT_TYPE) problems.push(`eventType must be ${EVENT_TYPE}`);
  if (event.eventVersion !== EVENT_VERSION) {
    throw new PermanentError(`Unsupported eventVersion ${JSON.stringify(event.eventVersion)} (supported: ${EVENT_VERSION})`);
  }
  if (!isJobId(event.jobId)) problems.push('jobId must be a UUID');
  const f = event.file;
  if (f === null || typeof f !== 'object') {
    problems.push('file must be an object');
  } else {
    if (typeof f.bucket !== 'string' || !f.bucket) problems.push('file.bucket must be a non-empty string');
    if (typeof f.key !== 'string' || !f.key.startsWith('uploads/')) problems.push('file.key must start with "uploads/"');
    if (typeof f.originalName !== 'string' || !f.originalName) problems.push('file.originalName must be a non-empty string');
    if (!CONTENT_TYPES.includes(f.contentType)) problems.push(`file.contentType must be one of ${CONTENT_TYPES.join(', ')}`);
    if (!Number.isInteger(f.size) || f.size < 0) problems.push('file.size must be a non-negative integer');
  }
  if (typeof event.uploadedAt !== 'string' || Number.isNaN(Date.parse(event.uploadedAt))) {
    problems.push('uploadedAt must be an ISO-8601 timestamp');
  }
  if (problems.length) throw new PermanentError(`Invalid event: ${problems.join('; ')}`);
  return event;
}
