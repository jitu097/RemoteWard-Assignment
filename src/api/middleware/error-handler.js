import { AppError } from '../../utils/errors.js';

export const notFoundHandler = (req, res) => {
  res.status(404).json({ success: false, error: { code: 'NOT_FOUND', message: 'Route not found' } });
};

export const errorHandler = (logger) => (err, req, res, next) => {
  if (res.headersSent) return next(err);
  if (err instanceof AppError) {
    return res.status(err.status).json({ success: false, error: { code: err.code, message: err.message } });
  }
  logger.error('request.unhandled_error', { err, path: req.path });
  res.status(500).json({ success: false, error: { code: 'INTERNAL_ERROR', message: 'Internal server error' } });
};
