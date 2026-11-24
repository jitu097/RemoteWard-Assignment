import express from 'express';
import { errorHandler, notFoundHandler } from './middleware/error-handler.js';

export function createApp({ config, logger }) {
  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', config.trustProxy);

  app.get('/health', (req, res) => res.json({ status: 'ok' }));

  app.use(notFoundHandler);
  app.use(errorHandler(logger));
  return app;
}
