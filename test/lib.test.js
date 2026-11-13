'use strict';

const test = require('node:test');
const assert = require('node:assert');
const { decode } = require('@msgpack/msgpack');
const { encodeColumns } = require('../lib/msgpack');
const { RowTyper, RowError, buildColumns } = require('../lib/rows');
const { toMicros } = require('../lib/time');
const { classify } = require('../lib/retry');
const names = require('../lib/names');

// Wire type of each cell of a column, read from the raw bytes, so a test can
// tell float64 21.0 from int 21 (a decoder cannot).
function wireTypes(buf, columnName) {
  const name = Buffer.from(columnName);
  const key = Buffer.concat([Buffer.from([0xa0 | name.length]), name]);
  const idx = buf.lastIndexOf(key);
  assert.ok(idx > 0, 'column not found');
  let p = idx + key.length;
  const head = buf[p];
  let n;
  if ((head & 0xf0) === 0x90) {
    n = head & 0x0f;
    p += 1;
  } else if (head === 0xdc) {
    n = buf.readUInt16BE(p + 1);
    p += 3;
  } else throw new Error('not an array');
  const types = [];
  for (let i = 0; i < n; i++) {
    const t = buf[p];
    if (t === 0xcb) { types.push('f64'); p += 9; } else if (t === 0xd3) { types.push('i64'); p += 9; } else if (t === 0xc0) { types.push('nil'); p += 1; } else if (t === 0xc2 || t === 0xc3) { types.push('bool'); p += 1; } else if ((t & 0xe0) === 0xa0) { types.push('str'); p += 1 + (t & 0x1f); } else throw new Error('unexpected byte ' + t.toString(16));
  }
  return types;
}

function encodeMsg(typer, msg, defaults) {
  const t = typer.type(msg, defaults || { measurement: 'm', database: 'db' });
  t.commit();
  const { groups } = t;
  const g = groups[0];
  return encodeColumns(g.measurement, buildColumns(g.rows));
}

test('integer-valued numbers are sent as float64, so 21 then 21.7 stay one float column', () => {
  const typer = new RowTyper({});
  const buf = encodeMsg(typer, { payload: [{ t: 21 }, { t: 21.7 }, { t: 0 }] });
  assert.deepStrictEqual(wireTypes(buf, 't'), ['f64', 'f64', 'f64']);
  const obj = decode(buf, { useBigInt64: true });
  assert.deepStrictEqual(obj.columns.t, [21, 21.7, 0]);
  assert.strictEqual(obj.m, 'm');
});

test('declared integer columns and BigInt are int64; time is int64 microseconds', () => {
  const typer = new RowTyper({ integerColumns: ['count'] });
  const buf = encodeMsg(typer, { payload: [{ count: 3, big: 9007199254740993n, time: 1700000000000 }] });
  assert.deepStrictEqual(wireTypes(buf, 'count'), ['i64']);
  assert.deepStrictEqual(wireTypes(buf, 'big'), ['i64']);
  assert.deepStrictEqual(wireTypes(buf, 'time'), ['i64']);
  const obj = decode(buf, { useBigInt64: true });
  assert.strictEqual(obj.columns.time[0], 1700000000000000n);
  assert.strictEqual(obj.columns.big[0], 9007199254740993n);
});

test('a non-integer in a declared integer column is a type mismatch, not a truncation', () => {
  const typer = new RowTyper({ integerColumns: ['count'] });
  assert.throws(() => typer.type({ payload: { count: 1.5 } }, { measurement: 'm' }), RowError);
});

test('type lock: a later value of another type is nulled by default', () => {
  const typer = new RowTyper({});
  typer.type({ payload: { v: 1.5 } }, { measurement: 'm' }).commit();
  const r = typer.type({ payload: { v: 'N/A', ok: true } }, { measurement: 'm' });
  assert.strictEqual(r.warnings.length, 1);
  assert.strictEqual(r.groups[0].rows[0].cells.has('v'), false);
  assert.strictEqual(r.groups[0].rows[0].cells.get('ok').cell, true);
});

test('type lock: reject-row rejects the message', () => {
  const typer = new RowTyper({ onTypeMismatch: 'reject-row' });
  typer.type({ payload: { v: 1.5 } }, { measurement: 'm' }).commit();
  assert.throws(() => typer.type({ payload: { v: 'x' } }, { measurement: 'm' }), RowError);
});

test('a rejected message does not lock types', () => {
  const typer = new RowTyper({});
  assert.throws(() => typer.type({ payload: [{ v: 'x' }, { _bad: 1 }] }, { measurement: 'm' }), RowError);
  const r = typer.type({ payload: { v: 2 } }, { measurement: 'm' });
  assert.strictEqual(r.warnings.length, 0);
  assert.strictEqual(r.groups[0].rows[0].cells.get('v').kind, 'float');
});

test('missing cells are nil; all-nil columns are left out', () => {
  const typer = new RowTyper({});
  const buf = encodeMsg(typer, { payload: [{ a: 1, b: NaN }, { b: null, c: 'x' }] });
  const obj = decode(buf, { useBigInt64: true });
  assert.deepStrictEqual(Object.keys(obj.columns).sort(), ['a', 'c', 'time']);
  assert.deepStrictEqual(obj.columns.a, [1, null]);
  assert.deepStrictEqual(obj.columns.c, [null, 'x']);
});

test('rows without time get distinct increasing microseconds', () => {
  const typer = new RowTyper({});
  const { groups } = typer.type({ payload: [{ a: 1 }, { a: 2 }, { a: 3 }] }, { measurement: 'm' });
  const ts = groups[0].rows.map((r) => r.timeUs);
  assert.ok(ts[0] < ts[1] && ts[1] < ts[2]);
});

test('name validation', () => {
  assert.ok(names.checkMeasurement('a/b'));
  assert.ok(names.checkMeasurement('1abc'));
  assert.strictEqual(names.checkMeasurement('boiler_1-a'), null);
  assert.ok(names.checkDatabase('system'));
  assert.ok(names.checkColumn('TIME'));
  assert.ok(names.checkColumn('_x'));
  assert.strictEqual(names.sanitize('plant/line 3.temp'), 'plant_line_3_temp');
  const typer = new RowTyper({});
  assert.throws(() => typer.type({ payload: { Temp: 1, temp: 2 } }, { measurement: 'm' }), /differ only by case/);
  assert.throws(() => typer.type({ payload: { a: 1 } }, { measurement: 'bad name' }), RowError);
  assert.throws(() => typer.type({ payload: { a: { nested: 1 } } }, { measurement: 'm' }), /nested/);
});

test('time conversion', () => {
  assert.strictEqual(toMicros(1700000000, 's').us, 1700000000000000n);
  assert.strictEqual(toMicros(1700000000.5, 's').us, 1700000000500000n);
  assert.strictEqual(toMicros(1700000000123, 'ms').us, 1700000000123000n);
  assert.strictEqual(toMicros('1700000000123456789', 'ns').us, 1700000000123456n);
  assert.strictEqual(toMicros(1700000000123456789n, 'ns').us, 1700000000123456n);
  assert.ok(toMicros(1700000000123456789, 'ns').error, 'unsafe ns number must be rejected');
  assert.strictEqual(toMicros('2023-11-14T22:13:20.123456Z', 'ms').us, 1700000000123456n);
  assert.strictEqual(toMicros(new Date(1700000000123), 'ms').us, 1700000000123000n);
  assert.ok(toMicros(1700000000123, 's').error, 'ms value with s precision is out of range');
  assert.ok(toMicros('yesterday', 'ms').error);
});

test('retry classification', () => {
  const r = (status, headers, body) => classify({ status, headers: headers || {}, body: Buffer.from(body || '') });
  assert.strictEqual(r(204).action, 'ok');
  assert.strictEqual(r(503, { 'x-arc-ingest-fenced': '1', 'retry-after': '5' }).action, 'resend');
  assert.strictEqual(r(503, { 'x-arc-ingest-fenced': '1', 'retry-after': '5' }).retryAfterMs, 5000);
  assert.strictEqual(r(503, { 'x-arc-ingest-refused': 'wal_disk' }).action, 'resend');
  assert.strictEqual(r(503, { 'x-arc-ingest-refused': 'wal_not_durable' }).action, 'fail');
  assert.strictEqual(r(503, {}, '{"error":"Write rejected (server shutting down)"}').action, 'fail');
  const pw = r(400, {}, '{"error":"partial write: 3 points dropped: bad; the other points were written"}');
  assert.strictEqual(pw.action, 'fail');
  assert.strictEqual(pw.dropped, 3);
  assert.strictEqual(r(500, {}, '{"error":"x","rows_written":10}').action, 'fail');
  assert.strictEqual(r(413).action, 'split');
  assert.strictEqual(r(401).action, 'auth');
  assert.strictEqual(r(403).action, 'auth');
  assert.strictEqual(r(502).action, 'fail');
  assert.strictEqual(r(504).action, 'fail');
  const net = (code, bodySent) => classify({ error: Object.assign(new Error(code), { code, bodySent }) });
  assert.strictEqual(net('ECONNREFUSED', false).action, 'resend');
  assert.strictEqual(net('ECONNRESET', true).action, 'fail');
  assert.strictEqual(net('ETIMEDOUT', true).action, 'fail');
});

test('columns named after Object.prototype members are ordinary columns', () => {
  const typer = new RowTyper({});
  const buf = encodeMsg(typer, { payload: { constructor: 1, toString: 2, valueOf: 'x', hasOwnProperty: true } });
  const obj = decode(buf, { useBigInt64: true });
  assert.deepStrictEqual(Object.keys(obj.columns).sort(), ['constructor', 'hasOwnProperty', 'time', 'toString', 'valueOf']);
  assert.deepStrictEqual(obj.columns.constructor, [1]);
  const cols = encodeMsg(new RowTyper({}), { payload: { columns: { constructor: [1, 2], v: [3, 4] } } });
  assert.deepStrictEqual(decode(cols).columns.constructor, [1, 2]);
  assert.throws(() => new RowTyper({}).type({ payload: JSON.parse('{"__proto__": 1, "v": 2}') }, { measurement: 'm' }), RowError);
});

test('names that differ only by case: rejected within a row and across messages', () => {
  const typer = new RowTyper({});
  typer.type({ payload: { Temp: 1 } }, { measurement: 'm' }).commit();
  assert.throws(() => typer.type({ payload: { temp: 2 } }, { measurement: 'm' }), /already written as "Temp"/);
  assert.throws(() => typer.type({ payload: [{ Temp: 1 }, { temp: 2 }] }, { measurement: 'n' }), /already written|same column/);
  // Arc folds ASCII only: non-ASCII case variants are different columns.
  typer.type({ payload: { 'Ä': 1, 'ä': 2 } }, { measurement: 'u' }).commit();
});

test('sanitizeNames collisions are rejected, integerColumns match raw names', () => {
  const typer = new RowTyper({ sanitizeNames: true, integerColumns: ['line.no'] });
  assert.throws(() => typer.type({ payload: { 'a.b': 1, a_b: 2 } }, { measurement: 'm' }), /same column/);
  const r = typer.type({ payload: { 'line.no': 3 } }, { measurement: 'm' });
  assert.strictEqual(r.groups[0].rows[0].cells.get('line_no').kind, 'int');
});

test('locks are not taken until commit', () => {
  const typer = new RowTyper({});
  typer.type({ payload: { v: 'x' } }, { measurement: 'm' }); // not committed (buffer refused it)
  const r = typer.type({ payload: { v: 1 } }, { measurement: 'm' });
  assert.strictEqual(r.warnings.length, 0);
});

test('time strings: ISO only', () => {
  assert.ok(toMicros('1.5', 's').error);
  assert.ok(toMicros('12/31', 'ms').error);
  assert.strictEqual(toMicros('2023-11-14T23:13:20.123456+01:00', 'ms').us, 1700000000123456n);
  assert.strictEqual(toMicros('2023-11-14 22:13:20', 'ms').us, 1700000000000000n);
  assert.ok(toMicros(1, 'constructor').error);
});

test('transport errors: anything before the body was sent is resent', () => {
  const net = (code, bodySent) => classify({ error: Object.assign(new Error(code), { code, bodySent }) });
  assert.strictEqual(net('ETIMEDOUT', false).action, 'resend');
  assert.strictEqual(net('ECONNRESET', false).action, 'resend');
  assert.strictEqual(net('ETIMEDOUT', true).action, 'fail');
});

test('query placeholders: quoting, negatives, refused positions', () => {
  const { render } = require('../nodes/arc-query.js');
  assert.strictEqual(render('SELECT 1 WHERE a-{{n}} > 0 AND t = 1', { n: -5 }), 'SELECT 1 WHERE a-(-5) > 0 AND t = 1');
  assert.strictEqual(render('SELECT 1 WHERE s = {{s}}', { s: "a' OR 1=1 --" }), "SELECT 1 WHERE s = 'a'' OR 1=1 --'");
  assert.strictEqual(render('SELECT {{#c}} FROM t WHERE id IN {{ids}}', { c: 'v', ids: [1, 'x'] }), 'SELECT "v" FROM t WHERE id IN ((1), \'x\')');
  assert.throws(() => render("SELECT 1 WHERE x LIKE '%{{q}}%'", { q: 'x' }), /inside quotes/);
  assert.throws(() => render('SELECT $$ {{q}} $$', { q: 'x' }), /inside quotes/);
  assert.throws(() => render('SELECT 1 -- {{q}}', { q: 'x' }), /comments/);
  assert.throws(() => render('SELECT 1 /* {{q}} */', { q: 'x' }), /comments/);
  assert.throws(() => render('SELECT {{#c}}', { c: 'a; DROP' }), /identifier/);
  assert.throws(() => render('SELECT {{q}}', {}), /missing/);
  assert.throws(() => render('SELECT {{q}}', { q: [] }), /empty/);
});
