const int = (name, def, { min = 0 } = {}) => (env, errors) => {
  const raw = env[name];
  if (raw === undefined || raw === '') return def;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < min) errors.push(`${name} must be an integer >= ${min}`);
  return n;
};
const str = (name, def) => (env, errors) => {
  const v = env[name] ?? def;
  if (v === undefined || v === '') {
    errors.push(`${name} is required`);
    return undefined;
  }
  return v;
};
const optional = (name) => (env) => env[name] || undefined;
const bool = (name, def) => (env) => (env[name] === undefined ? def : env[name] === 'true');

const schema = {
  nodeEnv: str('NODE_ENV', 'development'),
  logLevel: str('LOG_LEVEL', 'info'),
  port: int('PORT', 3000, { min: 1 }),
  trustProxy: int('TRUST_PROXY', 0),
  aws: {
    region: str('AWS_REGION'),
    endpoint: optional('AWS_ENDPOINT_URL'),
    forcePathStyle: bool('S3_FORCE_PATH_STYLE', false),
  },
  s3: { bucket: str('S3_BUCKET') },
  sqs: {
    queueUrl: str('SQS_QUEUE_URL'),
    dlqUrl: str('SQS_DLQ_URL'),
    visibilityTimeoutSeconds: int('SQS_VISIBILITY_TIMEOUT_SECONDS', 120, { min: 1 }),
    maxReceiveCount: int('SQS_MAX_RECEIVE_COUNT', 3, { min: 1 }),
  },
  redis: { url: str('REDIS_URL') },
  jobs: {
    statusTtlSeconds: int('JOB_STATUS_TTL_SECONDS', 86400, { min: 1 }),
    resultTtlSeconds: int('JOB_RESULT_TTL_SECONDS', 86400, { min: 1 }),
    lockTtlSeconds: int('JOB_LOCK_TTL_SECONDS', 60, { min: 5 }),
  },
  upload: { maxBytes: int('MAX_UPLOAD_BYTES', 10 * 1024 * 1024, { min: 1 }) },
  rateLimit: {
    upload: {
      max: int('UPLOAD_RATE_LIMIT_MAX', 30, { min: 1 }),
      windowMs: int('UPLOAD_RATE_LIMIT_WINDOW_MS', 15 * 60 * 1000, { min: 1 }),
    },
    read: {
      max: int('READ_RATE_LIMIT_MAX', 60, { min: 1 }),
      windowMs: int('READ_RATE_LIMIT_WINDOW_MS', 60 * 1000, { min: 1 }),
    },
  },
  worker: {
    concurrency: int('WORKER_CONCURRENCY', 2, { min: 1 }),
    shutdownTimeoutMs: int('WORKER_SHUTDOWN_TIMEOUT_MS', 30000, { min: 1 }),
    healthPort: int('WORKER_HEALTH_PORT', 8080, { min: 1 }),
  },
};

function build(node, env, errors) {
  if (typeof node === 'function') return node(env, errors);
  return Object.fromEntries(Object.entries(node).map(([k, v]) => [k, build(v, env, errors)]));
}

/** Parses and validates environment variables. Throws one error listing every problem. */
export function loadConfig(env = process.env) {
  const errors = [];
  const config = build(schema, env, errors);
  if (config.sqs.visibilityTimeoutSeconds !== undefined && config.jobs.lockTtlSeconds >= config.sqs.visibilityTimeoutSeconds) {
    errors.push('JOB_LOCK_TTL_SECONDS must be lower than SQS_VISIBILITY_TIMEOUT_SECONDS');
  }
  if (errors.length) throw new Error(`Invalid configuration: ${errors.join('; ')}`);
  return Object.freeze(config);
}
