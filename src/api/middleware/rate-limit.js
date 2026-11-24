import { rateLimit } from 'express-rate-limit';
import { RedisStore } from 'rate-limit-redis';

/**
 * Builds the upload and read limiters. Counters live in Redis, so the limits are shared by every API
 * instance. If Redis is unreachable the request is allowed through (fail open) and the error is logged:
 * a Redis outage then costs rate limiting, not API availability. There is deliberately no in-memory fallback,
 * because per-instance counters would silently give each replica its own, multiplied limit.
 */
export function createRateLimiters({ redis, config, logger }) {
  const build = (name, { max, windowMs }) =>
    rateLimit({
      windowMs,
      limit: max,
      standardHeaders: 'draft-7', // RateLimit + RateLimit-Policy headers
      legacyHeaders: false,
      passOnStoreError: true,
      store: new RedisStore({
        prefix: `rl:${name}:`,
        sendCommand: (command, ...args) => redis.call(command, ...args),
      }),
      handler: (req, res) => {
        logger.warn('rate_limit.exceeded', { limiter: name, path: req.path });
        res.status(429).json({
          success: false,
          error: { code: 'RATE_LIMITED', message: 'Too many requests, please retry later' },
        });
      },
    });

  return { upload: build('upload', config.rateLimit.upload), read: build('read', config.rateLimit.read) };
}
