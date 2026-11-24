import express from 'express';
import { createRoutes } from './routes/index.js';
import { errorHandler, notFoundHandler } from './middleware/error-handler.js';

/** `s3`, `queue` and `jobs` are injected so tests can substitute fakes. */
export function createApp({ config, logger, s3, queue, jobs }) {
  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', config.trustProxy);

  app.get('/health', (req, res) => res.json({ status: 'ok' }));
  app.use('/api/v1', createRoutes({ config, logger, s3, queue, jobs }));

  app.use(notFoundHandler);
  app.use(errorHandler(logger));
  return app;
}
