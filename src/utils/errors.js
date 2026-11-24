/** Error with an HTTP status and a stable machine-readable code. */
export class AppError extends Error {
  constructor(status, code, message) {
    super(message);
    this.name = 'AppError';
    this.status = status;
    this.code = code;
  }
}

/** The input can never succeed (bad event, malformed file). The worker must not retry it. */
export class PermanentError extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = 'PermanentError';
  }
}

export const errors = {
  badRequest: (code, message) => new AppError(400, code, message),
  notFound: (code, message) => new AppError(404, code, message),
  tooLarge: (maxBytes) => new AppError(413, 'FILE_TOO_LARGE', `File exceeds the ${maxBytes} byte limit`),
  unsupported: (message) => new AppError(415, 'UNSUPPORTED_FILE_TYPE', message),
  unavailable: (code, message) => new AppError(503, code, message),
};
