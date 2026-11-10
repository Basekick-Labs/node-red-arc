'use strict';

// Minimal MessagePack encoder for Arc's columnar write payload.
//
// The payload has one fixed shape: a map of string keys to strings, maps and
// arrays of typed cells. Every cell is written with an explicit wire type, which
// is the reason this encoder exists: a generic encoder picks int or float from
// the JS value, and Arc fixes a column's type from the first non-nil value of a
// request, truncating later floats in an int column. So the caller decides:
//
//   Float(x)  -> float64 (0xcb), even when x is integer-valued
//   bigint    -> int64   (0xd3)
//   string    -> str
//   boolean   -> bool
//   null      -> nil
//   Array     -> array
//   object    -> map (string keys)
//
// A bare JS number is rejected; wrap it with Float() or pass a bigint.

class Float {
  constructor(value) {
    this.value = value;
  }
}

class Writer {
  constructor(size) {
    this.buf = Buffer.allocUnsafe(size || 4096);
    this.pos = 0;
  }

  ensure(n) {
    if (this.pos + n <= this.buf.length) return;
    let size = this.buf.length * 2;
    while (size < this.pos + n) size *= 2;
    const next = Buffer.allocUnsafe(size);
    this.buf.copy(next, 0, 0, this.pos);
    this.buf = next;
  }

  byte(b) {
    this.ensure(1);
    this.buf[this.pos++] = b;
  }

  nil() {
    this.byte(0xc0);
  }

  bool(v) {
    this.byte(v ? 0xc3 : 0xc2);
  }

  float64(v) {
    this.ensure(9);
    this.buf[this.pos++] = 0xcb;
    this.buf.writeDoubleBE(v, this.pos);
    this.pos += 8;
  }

  int64(v) {
    this.ensure(9);
    this.buf[this.pos++] = 0xd3;
    this.buf.writeBigInt64BE(v, this.pos);
    this.pos += 8;
  }

  str(s) {
    const len = Buffer.byteLength(s, 'utf8');
    if (len < 32) {
      this.ensure(1 + len);
      this.buf[this.pos++] = 0xa0 | len;
    } else if (len < 0x100) {
      this.ensure(2 + len);
      this.buf[this.pos++] = 0xd9;
      this.buf[this.pos++] = len;
    } else if (len < 0x10000) {
      this.ensure(3 + len);
      this.buf[this.pos++] = 0xda;
      this.buf.writeUInt16BE(len, this.pos);
      this.pos += 2;
    } else {
      this.ensure(5 + len);
      this.buf[this.pos++] = 0xdb;
      this.buf.writeUInt32BE(len, this.pos);
      this.pos += 4;
    }
    this.buf.write(s, this.pos, len, 'utf8');
    this.pos += len;
  }

  arrayHeader(n) {
    if (n < 16) {
      this.byte(0x90 | n);
    } else if (n < 0x10000) {
      this.ensure(3);
      this.buf[this.pos++] = 0xdc;
      this.buf.writeUInt16BE(n, this.pos);
      this.pos += 2;
    } else {
      this.ensure(5);
      this.buf[this.pos++] = 0xdd;
      this.buf.writeUInt32BE(n, this.pos);
      this.pos += 4;
    }
  }

  mapHeader(n) {
    if (n < 16) {
      this.byte(0x80 | n);
    } else if (n < 0x10000) {
      this.ensure(3);
      this.buf[this.pos++] = 0xde;
      this.buf.writeUInt16BE(n, this.pos);
      this.pos += 2;
    } else {
      this.ensure(5);
      this.buf[this.pos++] = 0xdf;
      this.buf.writeUInt32BE(n, this.pos);
      this.pos += 4;
    }
  }

  value(v) {
    if (v === null || v === undefined) return this.nil();
    if (v instanceof Float) return this.float64(v.value);
    switch (typeof v) {
      case 'bigint':
        return this.int64(v);
      case 'string':
        return this.str(v);
      case 'boolean':
        return this.bool(v);
      case 'number':
        throw new TypeError('msgpack: bare number; wrap it in Float() or pass a bigint');
      default:
        break;
    }
    if (Array.isArray(v)) {
      this.arrayHeader(v.length);
      for (const item of v) this.value(item);
      return undefined;
    }
    if (typeof v === 'object') {
      const keys = Object.keys(v);
      this.mapHeader(keys.length);
      for (const k of keys) {
        this.str(k);
        this.value(v[k]);
      }
      return undefined;
    }
    throw new TypeError('msgpack: unsupported value of type ' + typeof v);
  }

  result() {
    return this.buf.subarray(0, this.pos);
  }
}

function encode(value, sizeHint) {
  const w = new Writer(sizeHint);
  w.value(value);
  return w.result();
}

// Typed column helpers: encode a whole column without allocating a wrapper per
// cell. kind is 'float' | 'int' | 'string' | 'bool'; cells are JS numbers for
// float, bigints for int, null for a missing cell.
function encodeColumns(measurement, columns, sizeHint) {
  const w = new Writer(sizeHint);
  w.mapHeader(2);
  w.str('m');
  w.str(measurement);
  w.str('columns');
  const names = Object.keys(columns);
  w.mapHeader(names.length);
  for (const name of names) {
    const col = columns[name];
    w.str(name);
    w.arrayHeader(col.values.length);
    for (const cell of col.values) {
      if (cell === null || cell === undefined) {
        w.nil();
        continue;
      }
      switch (col.kind) {
        case 'float':
          w.float64(cell);
          break;
        case 'int':
          w.int64(cell);
          break;
        case 'string':
          w.str(cell);
          break;
        case 'bool':
          w.bool(cell);
          break;
        default:
          throw new TypeError('msgpack: unknown column kind ' + col.kind);
      }
    }
  }
  return w.result();
}

module.exports = { encode, encodeColumns, Float };
