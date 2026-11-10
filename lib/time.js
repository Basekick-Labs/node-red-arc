'use strict';

// Every timestamp is converted to integer microseconds on the client. Arc infers
// the unit of a msgpack timestamp from its magnitude; sending microseconds that
// were computed here keeps that inference out of the picture.

const UNIT_TO_US = { s: 1000000n, ms: 1000n, us: 1n, ns: null };
const FIFTY_YEARS_US = 50n * 365n * 24n * 3600n * 1000000n;

function nowUs() {
  return BigInt(Date.now()) * 1000n;
}

// A clock that never repeats: rows enqueued in the same millisecond get
// distinct, increasing microsecond timestamps.
class MonotonicClock {
  constructor() {
    this.last = 0n;
  }

  next() {
    let t = nowUs();
    if (t <= this.last) t = this.last + 1n;
    this.last = t;
    return t;
  }
}

function fromBigInt(v, precision) {
  if (precision === 'ns') return v / 1000n;
  return v * UNIT_TO_US[precision];
}

// Convert a user-supplied time to microseconds. Returns {us} or {error}.
function toMicros(value, precision) {
  if (!(precision in UNIT_TO_US)) return { error: `unknown precision "${precision}"` };
  let us;
  if (value instanceof Date) {
    const ms = value.getTime();
    if (Number.isNaN(ms)) return { error: 'invalid Date' };
    us = BigInt(ms) * 1000n;
  } else if (typeof value === 'bigint') {
    us = fromBigInt(value, precision);
  } else if (typeof value === 'number') {
    if (!Number.isFinite(value)) return { error: `time ${value} is not finite` };
    if (precision === 's' || precision === 'ms') {
      // Fractions are meaningful here (1.5 s); keep microsecond resolution.
      const factor = precision === 's' ? 1e6 : 1e3;
      const scaled = Math.round(value * factor);
      if (!Number.isSafeInteger(scaled)) return { error: `time ${value} is out of range` };
      us = BigInt(scaled);
    } else {
      if (!Number.isSafeInteger(value)) {
        return { error: `time ${value} is not a safe integer; pass ${precision} timestamps as a BigInt or a string` };
      }
      us = fromBigInt(BigInt(value), precision);
    }
  } else if (typeof value === 'string') {
    const s = value.trim();
    if (/^-?\d+$/.test(s)) {
      us = fromBigInt(BigInt(s), precision);
    } else {
      const parsed = parseIso(s);
      if (parsed === null) return { error: `cannot parse time "${value}"` };
      us = parsed;
    }
  } else {
    return { error: `unsupported time value of type ${typeof value}` };
  }
  const now = nowUs();
  if (us < now - FIFTY_YEARS_US || us > now + FIFTY_YEARS_US) {
    return { error: `time ${value} is more than 50 years from now; check the precision setting (${precision})` };
  }
  return { us };
}

// ISO-8601 with up to microsecond (or nanosecond, truncated) fractions. Date.parse
// alone would drop everything below a millisecond.
function parseIso(s) {
  const m = /^(.*?[T ]\d{2}:\d{2}:\d{2})(?:[.,](\d+))?(Z|[+-]\d{2}:?\d{2})?$/i.exec(s);
  if (!m) {
    const ms = Date.parse(s);
    return Number.isNaN(ms) ? null : BigInt(ms) * 1000n;
  }
  const base = Date.parse(m[1].replace(' ', 'T') + (m[3] || 'Z'));
  if (Number.isNaN(base)) return null;
  const frac = (m[2] || '').slice(0, 6).padEnd(6, '0');
  return BigInt(base) * 1000n + BigInt(frac);
}

module.exports = { toMicros, MonotonicClock, nowUs };
