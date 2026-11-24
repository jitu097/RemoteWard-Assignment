const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

/** Serialises an error to name, message, code and stack only. */
export function errorFields(err) {
  if (!err) return undefined;
  return { name: err.name, message: err.message, code: err.code, stack: err.stack };
}

export function createLogger({ service, level = 'info', write = (line) => process.stdout.write(line + '\n') }) {
  const threshold = LEVELS[level] ?? LEVELS.info;
  const log = (lvl, event, fields = {}) => {
    if (LEVELS[lvl] < threshold) return;
    const { err, ...rest } = fields;
    write(JSON.stringify({ timestamp: new Date().toISOString(), level: lvl, service, event, ...rest, error: errorFields(err) }));
  };
  return {
    debug: (event, fields) => log('debug', event, fields),
    info: (event, fields) => log('info', event, fields),
    warn: (event, fields) => log('warn', event, fields),
    error: (event, fields) => log('error', event, fields),
  };
}
