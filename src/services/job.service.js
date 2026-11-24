export const STATUS = Object.freeze({
  QUEUED: 'queued',
  PROCESSING: 'processing',
  COMPLETED: 'completed',
  FAILED: 'failed',
  ENQUEUE_FAILED: 'enqueue_failed',
});

/**
 * Allowed transitions, keyed by target status. `completed` is terminal: nothing may leave it,
 * so a stale duplicate delivery can never overwrite a finished job.
 */
export const ALLOWED_FROM = Object.freeze({
  queued: ['enqueue_failed'], // creation, or recovery after a failed publish
  processing: ['queued', 'failed', 'processing'], // processing->processing: re-claim after a crashed worker
  completed: ['processing'],
  failed: ['processing'],
  enqueue_failed: ['queued'],
});

export const canTransition = (from, to) => ALLOWED_FROM[to]?.includes(from) ?? false;

export const keys = {
  status: (id) => `job:status:${id}`,
  result: (id) => `job:result:${id}`,
  lock: (id) => `job:lock:${id}`,
};

// Atomic check-and-set. KEYS: status, result. ARGV: allowedFrom(json), patch(json), newStatus,
// now, resultJson|'', resultTtl, statusTtl. Returns {1, record} or {0, currentStatus|'missing'}.
const TRANSITION_LUA = `
local raw = redis.call('GET', KEYS[1])
if not raw then return {0, 'missing'} end
local rec = cjson.decode(raw)
local ok = false
for _, s in ipairs(cjson.decode(ARGV[1])) do if s == rec.status then ok = true end end
if not ok then return {0, rec.status} end
for k, v in pairs(cjson.decode(ARGV[2])) do rec[k] = v end
rec.status = ARGV[3]
rec.updatedAt = ARGV[4]
if ARGV[3] == 'processing' then rec.attempts = (rec.attempts or 0) + 1 end
if ARGV[5] ~= '' then redis.call('SET', KEYS[2], ARGV[5], 'EX', tonumber(ARGV[6])) end
local out = cjson.encode(rec)
redis.call('SET', KEYS[1], out, 'EX', tonumber(ARGV[7]))
return {1, out}
`;

const RENEW_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2])) end
return 0
`;
const RELEASE_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`;

export function createJobService({ redis, config }) {
  const { statusTtlSeconds, resultTtlSeconds, lockTtlSeconds } = config.jobs;

  async function transition(jobId, to, { patch = {}, result } = {}) {
    const [ok, payload] = await redis.eval(
      TRANSITION_LUA,
      2,
      keys.status(jobId),
      keys.result(jobId),
      JSON.stringify(ALLOWED_FROM[to]),
      JSON.stringify(patch),
      to,
      new Date().toISOString(),
      result === undefined ? '' : JSON.stringify(result),
      resultTtlSeconds,
      statusTtlSeconds,
    );
    return ok === 1 ? { ok: true, record: JSON.parse(payload) } : { ok: false, current: payload };
  }

  return {
    transition,

    /** Creates the job in `queued` state. Returns false if it already exists. */
    async create(jobId, file) {
      const now = new Date().toISOString();
      const record = { jobId, status: STATUS.QUEUED, file, attempts: 0, createdAt: now, updatedAt: now };
      const res = await redis.set(keys.status(jobId), JSON.stringify(record), 'EX', statusTtlSeconds, 'NX');
      return res === 'OK' ? record : null;
    },

    async get(jobId) {
      const raw = await redis.get(keys.status(jobId));
      return raw ? JSON.parse(raw) : null;
    },

    async getResult(jobId) {
      const raw = await redis.get(keys.result(jobId));
      return raw ? JSON.parse(raw) : null;
    },

    markEnqueueFailed: (jobId, error) => transition(jobId, STATUS.ENQUEUE_FAILED, { patch: { error } }),
    requeue: (jobId) => transition(jobId, STATUS.QUEUED),
    markProcessing: (jobId) => transition(jobId, STATUS.PROCESSING),
    /** Stores the result and flips to completed in one atomic step. */
    complete: (jobId, result) =>
      transition(jobId, STATUS.COMPLETED, { patch: { completedAt: new Date().toISOString() }, result }),
    fail: (jobId, error) => transition(jobId, STATUS.FAILED, { patch: { error } }),

    /** Per-job processing lock. `token` identifies the holder so only it can renew or release. */
    async acquireLock(jobId, token) {
      return (await redis.set(keys.lock(jobId), token, 'EX', lockTtlSeconds, 'NX')) === 'OK';
    },
    async renewLock(jobId, token) {
      return (await redis.eval(RENEW_LUA, 1, keys.lock(jobId), token, lockTtlSeconds)) === 1;
    },
    async releaseLock(jobId, token) {
      return (await redis.eval(RELEASE_LUA, 1, keys.lock(jobId), token)) === 1;
    },
  };
}
