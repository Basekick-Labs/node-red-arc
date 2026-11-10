'use strict';

// Turns a Node-RED message into typed rows ready for the buffer.
//
// A row is a flat object of column -> value plus an optional "time". Arc has no
// tag/field split: every value is a column.
//
// Typing, which is where data is lost if it goes wrong:
//   - a JS number is a float64, even when integer-valued, unless its column is
//     declared in integerColumns (then it must be a safe integer and becomes
//     int64); a bigint is always int64
//   - string, boolean as-is; null/undefined are a missing cell
//   - NaN and +-Infinity become a missing cell and are counted
//   - nested objects and arrays are rejected unless stringifyObjects is on
//   - the first value seen for a (database, measurement, column) locks its type
//     for the life of the node; a later value of another type is handled by
//     onTypeMismatch: 'null' (that cell becomes missing, the rest of the row is
//     kept) or 'reject-row'

const names = require('./names');
const time = require('./time');

const KIND_LABEL = { float: 'float', int: 'integer', string: 'string', bool: 'boolean' };

class RowError extends Error {}

// Normalise msg.payload into an array of plain row objects plus the
// measurement each belongs to.
function extractRows(msg, defaults) {
  const payload = msg.payload;
  const measurement = msg.measurement || defaults.measurement;
  if (payload === null || payload === undefined) throw new RowError('msg.payload is empty');
  if (typeof payload === 'number' || typeof payload === 'bigint') {
    return [{ measurement, rows: [{ value: payload }] }];
  }
  if (Array.isArray(payload)) {
    for (const r of payload) {
      if (r === null || typeof r !== 'object' || Array.isArray(r)) {
        throw new RowError('msg.payload array must contain row objects');
      }
    }
    return [{ measurement, rows: payload }];
  }
  if (typeof payload !== 'object') {
    throw new RowError(`msg.payload of type ${typeof payload} is not a row; send an object, an array of objects or a number`);
  }
  if (Array.isArray(payload.rows)) {
    return [{ measurement: payload.measurement || measurement, rows: payload.rows }];
  }
  if (payload.columns && typeof payload.columns === 'object') {
    return [{ measurement: payload.measurement || measurement, rows: columnsToRows(payload.columns) }];
  }
  return [{ measurement, rows: [payload] }];
}

function columnsToRows(columns) {
  const keys = Object.keys(columns);
  if (keys.length === 0) throw new RowError('msg.payload.columns is empty');
  let n = -1;
  for (const k of keys) {
    if (!Array.isArray(columns[k])) throw new RowError(`column "${k}" is not an array`);
    if (n === -1) n = columns[k].length;
    else if (columns[k].length !== n) throw new RowError('all columns must have the same length');
  }
  const rows = new Array(n);
  for (let i = 0; i < n; i++) {
    const row = Object.create(null);
    for (const k of keys) row[k] = columns[k][i];
    rows[i] = row;
  }
  return rows;
}

function kindOf(value, isIntegerColumn, stringifyObjects) {
  switch (typeof value) {
    case 'number':
      if (!Number.isFinite(value)) return { missing: true, nonFinite: true };
      if (isIntegerColumn) {
        if (!Number.isSafeInteger(value)) return { error: `value ${value} is not a safe integer` };
        return { kind: 'int', cell: BigInt(value) };
      }
      return { kind: 'float', cell: value };
    case 'bigint':
      if (value > 9223372036854775807n || value < -9223372036854775808n) {
        return { error: `value ${value} does not fit in int64` };
      }
      return { kind: 'int', cell: value };
    case 'string':
      return { kind: 'string', cell: value.toWellFormed ? value.toWellFormed() : value };
    case 'boolean':
      return { kind: 'bool', cell: value };
    case 'object':
      if (value === null) return { missing: true };
      if (value instanceof Date) return { error: 'Date values are only accepted in the time column' };
      if (stringifyObjects) return { kind: 'string', cell: JSON.stringify(value) };
      return { error: 'nested objects and arrays are not columns; enable "stringify objects" or flatten them' };
    case 'undefined':
      return { missing: true };
    default:
      return { error: `unsupported value of type ${typeof value}` };
  }
}

// Arc folds column names by ASCII letter case only (FoldColumnName), so two
// spellings that differ in A-Z case are one column to Arc.
function fold(name) {
  return name.replace(/[A-Z]/g, (c) => c.toLowerCase());
}

// Holds the type lock and builds typed rows.
class RowTyper {
  constructor(opts) {
    this.integerColumns = new Set(opts.integerColumns || []);
    this.onTypeMismatch = opts.onTypeMismatch || 'null';
    this.stringifyObjects = !!opts.stringifyObjects;
    this.sanitizeNames = !!opts.sanitizeNames;
    this.precision = opts.precision || 'ms';
    this.clock = new time.MonotonicClock();
    // db \0 measurement \0 folded column -> {name, kind}: the first spelling and
    // type seen for a column fix both for the life of the node.
    this.locks = new Map();
    this.nonFinite = 0;
  }

  // Returns {groups: [{db, measurement, rows: [{timeUs, cells: Map}]}],
  // warnings, commit}. Throws RowError when the whole message must be rejected.
  // The column locks the message takes are applied only by commit(), which the
  // caller runs once the buffer has accepted the message.
  type(msg, defaults) {
    let db = msg.database || defaults.database || 'default';
    if (this.sanitizeNames) db = names.sanitize(db);
    const dbErr = names.checkDatabase(db);
    if (dbErr) throw new RowError(dbErr);
    const precision = msg.precision || this.precision;
    const groups = extractRows(msg, defaults);
    const out = [];
    const warnings = [];
    const pending = new Map();
    for (const g of groups) {
      let measurement = g.measurement;
      if (!measurement) throw new RowError('no measurement: set it on the node or in msg.measurement');
      if (this.sanitizeNames) measurement = names.sanitize(measurement);
      const mErr = names.checkMeasurement(measurement);
      if (mErr) throw new RowError(mErr);
      const typed = [];
      for (const row of g.rows) {
        typed.push(this.typeRow(row, db, measurement, msg, precision, warnings, pending));
      }
      out.push({ db, measurement, rows: typed });
    }
    const commit = () => {
      for (const [k, v] of pending) if (!this.locks.has(k)) this.locks.set(k, v);
    };
    return { groups: out, warnings, commit };
  }

  typeRow(row, db, measurement, msg, precision, warnings, pending) {
    if (row === null || typeof row !== 'object' || Array.isArray(row)) {
      throw new RowError('each row must be an object of column: value');
    }
    let timeUs;
    const t = Object.prototype.hasOwnProperty.call(row, 'time') ? row.time : msg.time;
    if (t === undefined || t === null) {
      timeUs = this.clock.next();
    } else {
      const r = time.toMicros(t, precision);
      if (r.error) throw new RowError(r.error);
      timeUs = r.us;
    }
    const cells = new Map();
    const seen = new Map(); // folded name -> raw name, within this row
    for (const rawName of Object.keys(row)) {
      if (rawName === 'time') continue;
      const name = this.sanitizeNames ? names.sanitize(rawName) : rawName;
      const cErr = names.checkColumn(name);
      if (cErr) throw new RowError(cErr);
      const f = fold(name);
      if (seen.has(f)) {
        throw new RowError(`columns "${seen.get(f)}" and "${rawName}" are the same column to Arc` +
          (this.sanitizeNames ? ' (after sanitizing or ignoring case)' : ' (names differ only by case)'));
      }
      seen.set(f, rawName);
      const isInt = this.integerColumns.has(name) || this.integerColumns.has(rawName);
      const k = kindOf(row[rawName], isInt, this.stringifyObjects);
      if (k.error) throw new RowError(`column "${name}": ${k.error}`);
      if (k.missing) {
        if (k.nonFinite) this.nonFinite++;
        continue;
      }
      const lockKey = db + '\0' + measurement + '\0' + f;
      const locked = this.locks.get(lockKey) || pending.get(lockKey);
      if (locked === undefined) {
        pending.set(lockKey, { name, kind: k.kind });
      } else {
        if (locked.name !== name) {
          throw new RowError(`column "${name}" of ${measurement} was already written as "${locked.name}"; Arc treats names that differ only by case as one column`);
        }
        if (locked.kind !== k.kind) {
          const text = `column "${name}" of ${measurement} is ${KIND_LABEL[locked.kind]}, got ${KIND_LABEL[k.kind]} ${JSON.stringify(String(row[rawName]))}`;
          if (this.onTypeMismatch === 'reject-row') throw new RowError(text);
          warnings.push({ column: name, measurement, expected: KIND_LABEL[locked.kind], value: row[rawName], message: text });
          continue;
        }
      }
      cells.set(name, { kind: k.kind, cell: k.cell });
    }
    if (cells.size === 0) throw new RowError(`row for ${measurement} has no values besides time`);
    return { timeUs, cells };
  }
}

// Build the columns of one request from typed rows. Columns whose cells are all
// missing are left out: Arc stores an all-nil column as a string column, which
// would clash with the same column's numeric type in other files. A
// prototype-less object, so a column called "constructor" or "toString" is
// just a column.
function buildColumns(rows) {
  const n = rows.length;
  const columns = Object.create(null);
  columns.time = { kind: 'int', values: new Array(n) };
  for (let i = 0; i < n; i++) {
    columns.time.values[i] = rows[i].timeUs;
    for (const [name, c] of rows[i].cells) {
      let col = columns[name];
      if (!col) {
        col = { kind: c.kind, values: new Array(n).fill(null) };
        columns[name] = col;
      }
      col.values[i] = c.cell;
    }
  }
  return columns;
}

// Rough size of a typed row in bytes, for the buffer's byte cap.
function estimateBytes(row) {
  let b = 16;
  for (const [name, c] of row.cells) {
    b += name.length + 2;
    b += c.kind === 'string' ? Buffer.byteLength(c.cell) + 5 : 9;
  }
  return b;
}

module.exports = { RowTyper, RowError, buildColumns, estimateBytes, extractRows };
