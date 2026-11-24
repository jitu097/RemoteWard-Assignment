import { parse } from 'csv-parse';
import { PermanentError } from '../../utils/errors.js';
import { createStats, MAX_COLUMNS } from './stats.js';
import { parsedRecords } from './stream.js';

/**
 * Streams a CSV (RFC 4180, UTF-8, optional BOM, first row = header).
 * Malformed input is fatal for the whole job (no partial results): duplicate or blank header names,
 * and any row whose field count differs from the header's.
 */
export async function processCsv(source, signal) {
  const stats = createStats({ numericStrings: true });
  const parser = parse({
    bom: true,
    skip_empty_lines: true,
    columns: (header) => {
      if (header.length > MAX_COLUMNS) throw new PermanentError(`More than ${MAX_COLUMNS} columns`);
      if (header.some((h) => h.trim() === '')) throw new PermanentError('CSV header contains a blank column name');
      if (new Set(header).size !== header.length) throw new PermanentError('CSV header contains duplicate column names');
      stats.declareColumns(header);
      return header;
    },
  });
  for await (const record of parsedRecords(source, parser, signal)) stats.add(record);
  return { fileType: 'csv', ...stats.result() };
}
