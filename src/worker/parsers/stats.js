import { PermanentError } from '../../utils/errors.js';

export const MAX_COLUMNS = 1000;
const NUMERIC = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
const round = (n) => Number(n.toFixed(6));

/**
 * Single-pass accumulator producing the result schema shared by CSV and JSON:
 * { totalRecords, totalColumns, columns, emptyValues, numericStats }.
 *
 * - "Empty" means null/undefined, or a string that is blank after trimming. A key missing from a record
 *   counts as empty for that record.
 * - A column gets numericStats only if every non-empty value in it is numeric. CSV values are strings,
 *   so `numericStrings` lets "12.5" count as a number; JSON strings are never coerced.
 * - Sums and means are rounded to 6 decimals; floating point accumulation error is not corrected.
 * - Memory is O(columns), independent of the number of records.
 */
export function createStats({ numericStrings }) {
  let totalRecords = 0;
  const cols = new Map();

  const col = (name) => {
    let c = cols.get(name);
    if (!c) {
      if (cols.size >= MAX_COLUMNS) throw new PermanentError(`More than ${MAX_COLUMNS} distinct columns`);
      c = { nonEmpty: 0, numeric: 0, min: Infinity, max: -Infinity, sum: 0 };
      cols.set(name, c);
    }
    return c;
  };

  return {
    declareColumns(names) {
      for (const n of names) col(n);
    },

    add(record) {
      totalRecords += 1;
      for (const [name, value] of Object.entries(record)) {
        const c = col(name);
        if (value === null || value === undefined) continue;
        let num = null;
        if (typeof value === 'string') {
          const t = value.trim();
          if (t === '') continue;
          if (numericStrings && NUMERIC.test(t)) num = Number(t);
        } else if (typeof value === 'number' && Number.isFinite(value)) {
          num = value;
        }
        c.nonEmpty += 1;
        if (num !== null) {
          c.numeric += 1;
          c.sum += num;
          if (num < c.min) c.min = num;
          if (num > c.max) c.max = num;
        }
      }
    },

    result() {
      const columns = [...cols.keys()];
      const emptyValues = {};
      const numericStats = {};
      for (const [name, c] of cols) {
        emptyValues[name] = totalRecords - c.nonEmpty;
        if (c.numeric > 0 && c.numeric === c.nonEmpty) {
          numericStats[name] = { count: c.numeric, min: c.min, max: c.max, sum: round(c.sum), mean: round(c.sum / c.numeric) };
        }
      }
      return { totalRecords, totalColumns: columns.length, columns, emptyValues, numericStats };
    },
  };
}
