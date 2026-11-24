import { loadConfig } from '../config/env.js';
import { createLogger } from '../utils/logger.js';
import { createApp } from './app.js';

const config = loadConfig();
const logger = createLogger({ service: 'api', level: config.logLevel });
const app = createApp({ config, logger });

const server = app.listen(config.port, () => logger.info('api.started', { port: config.port }));

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
    logger.info('api.shutdown', { signal });
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 10000).unref();
  });
}
