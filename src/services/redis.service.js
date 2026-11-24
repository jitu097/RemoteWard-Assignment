import { Redis } from 'ioredis';

/**
 * Creates a Redis client that reconnects forever and fails individual commands quickly while
 * disconnected. The 'error' listener is required: without it ioredis errors crash the process.
 */
export function createRedis({ url, logger }) {
  const redis = new Redis(url, { maxRetriesPerRequest: 1, retryStrategy: (n) => Math.min(n * 200, 5000) });
  redis.on('error', (err) => logger.warn('redis.error', { err }));
  redis.on('ready', () => logger.info('redis.ready'));
  redis.on('close', () => logger.warn('redis.connection_closed'));
  return redis;
}
