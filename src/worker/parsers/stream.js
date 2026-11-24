import { PermanentError } from '../../utils/errors.js';

/**
 * Pipes `source` through `parser` and yields parsed records.
 *
 * Errors are classified by origin: a failure of the source stream (S3 / network) is rethrown untouched so
 * the caller treats it as transient; a failure of the parser means the content is malformed and can never
 * succeed, so it becomes a PermanentError. Uses pipe() (which does not forward errors) to keep the origins apart.
 */
export async function* parsedRecords(source, parser, signal) {
  let sourceError;
  source.once('error', (err) => {
    sourceError = err;
    parser.destroy(err);
  });
  const onAbort = () => source.destroy(signal.reason);
  signal?.addEventListener('abort', onAbort, { once: true });
  source.pipe(parser);
  try {
    yield* parser;
  } catch (err) {
    if (err === sourceError || signal?.aborted) throw signal?.aborted ? signal.reason : err;
    if (err instanceof PermanentError) throw err;
    throw new PermanentError(`Malformed input: ${err.message}`, { cause: err });
  } finally {
    signal?.removeEventListener('abort', onAbort);
    source.destroy();
  }
}
