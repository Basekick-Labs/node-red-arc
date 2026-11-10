'use strict';

// Name rules mirrored from Arc. A name Arc rejects fails the whole request, so
// every name is checked before a row enters the buffer.
//   database:    internal/api/databases.go isValidDatabaseName
//   measurement: internal/api/lineprotocol.go isValidMeasurementName (also used
//                by the msgpack handler)

const DATABASE_RE = /^[A-Za-z][A-Za-z0-9_-]*$/;
const MEASUREMENT_RE = /^[a-zA-Z][a-zA-Z0-9_-]*$/;
const RESERVED_DATABASES = new Set(['system', 'internal', '_internal']);

function checkDatabase(name) {
  if (typeof name !== 'string' || name === '') return 'database name is empty';
  if (name.length > 64) return `database name "${name}" is longer than 64 characters`;
  if (!DATABASE_RE.test(name)) {
    return `invalid database name "${name}": must start with a letter and contain only letters, digits, "_" or "-"`;
  }
  if (RESERVED_DATABASES.has(name.toLowerCase())) return `database name "${name}" is reserved`;
  return null;
}

function checkMeasurement(name) {
  if (typeof name !== 'string' || name === '') return 'measurement name is empty';
  if (name.length > 128) return `measurement name "${name}" is longer than 128 characters`;
  if (!MEASUREMENT_RE.test(name)) {
    return `invalid measurement name "${name}": must start with a letter and contain only letters, digits, "_" or "-"`;
  }
  return null;
}

// Column names. "time" is the time column and handled by the caller; any other
// spelling of it is refused by Arc (#1342), as are names starting with "_",
// which Arc does not store.
function checkColumn(name) {
  if (typeof name !== 'string' || name === '') return 'column name is empty';
  if (name.toLowerCase() === 'time') return `column "${name}" collides with the time column`;
  if (name.startsWith('_')) return `column "${name}" starts with "_", which Arc does not store`;
  if (name.isWellFormed && !name.isWellFormed()) return `column "${name}" is not valid UTF-8`;
  return null;
}

// Replace characters Arc rejects with "_" and make sure the name starts with a
// letter. Used only when the node's sanitizeNames option is on.
function sanitize(name) {
  let s = String(name).replace(/[^A-Za-z0-9_-]/g, '_');
  if (!/^[A-Za-z]/.test(s)) s = 'm_' + s;
  return s;
}

module.exports = { checkDatabase, checkMeasurement, checkColumn, sanitize };
