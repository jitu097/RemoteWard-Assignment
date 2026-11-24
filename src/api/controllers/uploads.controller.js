import { randomUUID } from 'node:crypto';
import { pipeline } from 'node:stream';
import busboy from 'busboy';
import { AppError, errors } from '../../utils/errors.js';
import { buildFileUploadedEvent } from '../../utils/events.js';
import { cleanFileName, createFileGuard, resolveType } from '../middleware/file-guard.js';

const dayPrefix = (d) =>
  `uploads/${d.getUTCFullYear()}/${String(d.getUTCMonth() + 1).padStart(2, '0')}/${String(d.getUTCDate()).padStart(2, '0')}`;

/** Resolves with the first file part of a multipart request; the stream keeps flowing afterwards. */
function firstFilePart(req, maxBytes) {
  return new Promise((resolve, reject) => {
    let parser;
    try {
      // maxBytes + 1 so the guard (not busboy) detects the overflow and reports it precisely.
      parser = busboy({ headers: req.headers, limits: { files: 1, fields: 5, fileSize: maxBytes + 1 } });
    } catch {
      return reject(errors.unsupported('Request must be multipart/form-data with a "file" part'));
    }
    parser.once('file', (field, stream, info) => resolve({ stream, info }));
    parser.once('error', (err) => reject(errors.badRequest('MALFORMED_UPLOAD', `Malformed multipart body: ${err.message}`)));
    parser.once('close', () => reject(errors.badRequest('NO_FILE', 'No file part found in the request')));
    req.pipe(parser);
  });
}

export function createUploadController({ config, s3, queue, jobs, logger }) {
  return async (req, res) => {
    const declaredLength = Number(req.headers['content-length']);
    if (declaredLength > config.upload.maxBytes + 64 * 1024) throw errors.tooLarge(config.upload.maxBytes);

    const { stream, info } = await firstFilePart(req, config.upload.maxBytes);
    const fileName = cleanFileName(info.filename);
    let guard;
    const jobId = randomUUID();
    const now = new Date();
    try {
      const { ext, contentType } = resolveType(fileName, info.mimeType);
      const key = `${dayPrefix(now)}/${jobId}${ext}`;

      guard = createFileGuard({ contentType, maxBytes: config.upload.maxBytes });
      let size = 0;
      guard.on('data', (c) => (size += c.length)); // counted as the stream passes; no buffering
      let guardError;
      guard.once('error', (e) => (guardError = e));
      pipeline(stream, guard, () => {});

      try {
        await s3.upload({ key, body: guard, contentType });
      } catch (err) {
        if (guardError) throw guardError;
        logger.error('upload.s3_failed', { jobId, key, err });
        throw err instanceof AppError ? err : errors.unavailable('STORAGE_UNAVAILABLE', 'Could not store the file');
      }

      const file = { bucket: config.s3.bucket, key, originalName: fileName, contentType, size };

      // The object is in S3. From here a failure must never lose track of it.
      let record;
      try {
        record = await jobs.create(jobId, file);
      } catch (err) {
        logger.error('upload.orphaned_object', { jobId, key, bucket: file.bucket, err });
        throw errors.unavailable('JOB_STORE_UNAVAILABLE', 'Upload stored but job tracking is unavailable; please retry');
      }
      if (!record) throw new Error('job id collision');

      try {
        await queue.publish(buildFileUploadedEvent({ jobId, ...file, uploadedAt: now }));
      } catch (err) {
        logger.error('upload.enqueue_failed', { jobId, key, bucket: file.bucket, err });
        await jobs
          .markEnqueueFailed(jobId, { code: 'ENQUEUE_FAILED', message: String(err.message).slice(0, 500) })
          .catch((e) => logger.error('upload.enqueue_failure_not_recorded', { jobId, key, err: e }));
        const e = errors.unavailable('ENQUEUE_FAILED', `File stored but could not be queued. Job ${jobId} can be re-queued by an operator.`);
        throw e;
      }

      logger.info('upload.accepted', { jobId, key, size });
      res.status(202).json({
        success: true,
        message: 'File accepted for processing',
        data: { jobId, status: 'queued', fileName, statusUrl: `/api/v1/jobs/${jobId}` },
      });
    } catch (err) {
      stream.resume(); // drain the rest of the request so the response can be delivered
      res.set('Connection', 'close');
      throw err;
    }
  };
}
