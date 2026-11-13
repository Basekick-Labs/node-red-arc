'use strict';

const test = require('node:test');
const assert = require('node:assert');
const { decode } = require('@msgpack/msgpack');
const { Writer } = require('../lib/writer');
const { RowTyper } = require('../lib/rows');

function setup(opts, responder) {
  const posts = [];
  const typer = new RowTyper({});
  const writer = new Writer(Object.assign({
    flushIntervalMs: 20,
    batchSize: 1000,
    post: async (db, body) => {
      const decoded = decode(body, { useBigInt64: true });
      posts.push({ db, decoded });
      return responder(posts.length, decoded);
    },
  }, opts));
  const send = (payload, extra) => new Promise((resolve) => {
    const msg = Object.assign({ payload }, extra);
    const t = typer.type(msg, { measurement: 'm', database: 'db' });
    const h = writer.handle((err) => resolve(err ? Object.assign(err, { written: h.written }) : null));
    if (writer.enqueue(t.groups, h)) t.commit();
  });
  return { writer, posts, send };
}

const ok = () => ({ status: 204, headers: {}, body: Buffer.alloc(0) });

test('done only after 204; batches across messages', async () => {
  const { writer, posts, send } = setup({}, ok);
  const results = await Promise.all([send({ a: 1 }), send({ a: 2 }), send([{ a: 3 }, { a: 4 }])]);
  assert.deepStrictEqual(results, [null, null, null]);
  assert.strictEqual(posts.length, 1);
  assert.deepStrictEqual(posts[0].decoded.columns.a, [1, 2, 3, 4]);
  await writer.close(100);
});

test('a message spanning two measurements completes after both batches', async () => {
  const { writer, posts, send } = setup({}, ok);
  const err = await send({ a: 1 }, { measurement: 'x' }).then(() => send({ rows: [{ b: 1 }], measurement: 'y' }));
  assert.strictEqual(err, null);
  assert.strictEqual(posts.length, 2);
  await writer.close(100);
});

test('fenced 503 is resent and then succeeds', async () => {
  const { writer, posts, send } = setup({}, (n) => (n === 1
    ? { status: 503, headers: { 'x-arc-ingest-fenced': '1', 'retry-after': '0' }, body: Buffer.alloc(0) }
    : ok()));
  assert.strictEqual(await send({ a: 1 }), null);
  assert.strictEqual(posts.length, 2);
  await writer.close(100);
});

test('unmarked 503 (Arc 26.x) is reported, not resent', async () => {
  const { writer, posts, send } = setup({}, () => ({ status: 503, headers: {}, body: Buffer.from('{"error":"Write rejected (server shutting down)"}') }));
  const err = await send({ a: 1 });
  assert.ok(err);
  assert.match(err.message, /not resent/);
  assert.strictEqual(err.arc.status, 503);
  assert.strictEqual(posts.length, 1);
  await writer.close(100);
});

test('ambiguous transport error is reported; connect refused is resent', async () => {
  let n = 0;
  const { writer, posts, send } = setup({}, () => {
    n++;
    if (n === 1) throw Object.assign(new Error('refused'), { code: 'ECONNREFUSED', bodySent: false });
    if (n === 2) return ok();
    throw Object.assign(new Error('reset'), { code: 'ECONNRESET', bodySent: true });
  });
  writer.maxBackoffMs = 10;
  writer.keys.clear();
  const first = await send({ a: 1 });
  assert.strictEqual(first, null);
  const second = await send({ a: 2 });
  assert.ok(second);
  assert.strictEqual(posts.length, 3);
  await writer.close(100);
});

test('413 splits the batch until it fits', async () => {
  const { writer, posts, send } = setup({ flushIntervalMs: 50 }, (_, d) => (d.columns.a.length > 2 ? { status: 413, headers: {}, body: Buffer.alloc(0) } : ok()));
  const err = await send([{ a: 1 }, { a: 2 }, { a: 3 }, { a: 4 }, { a: 5 }]);
  assert.strictEqual(err, null);
  const written = posts.filter((p) => p.decoded.columns.a.length <= 2).flatMap((p) => p.decoded.columns.a);
  assert.deepStrictEqual(written, [1, 2, 3, 4, 5]);
  await writer.close(100);
});

test('401 pauses and resumes after the auth probe succeeds', async () => {
  let authOk = false;
  const { writer, posts, send } = setup({ authProbeMs: 30, authProbe: async () => authOk }, () => (authOk ? ok() : { status: 401, headers: {}, body: Buffer.alloc(0) }));
  const p = send({ a: 1 });
  await new Promise((r) => setTimeout(r, 60));
  assert.strictEqual(writer.paused, true);
  authOk = true;
  assert.strictEqual(await p, null);
  assert.ok(posts.length >= 2);
  await writer.close(100);
});

test('403 pauses the batch until the auth probe confirms write access', async () => {
  let canWrite = false;
  const { writer, posts, send } = setup({ authProbeMs: 25, authProbe: async () => canWrite }, () => (
    canWrite ? ok() : { status: 403, headers: {}, body: Buffer.alloc(0) }
  ));
  const p = send({ a: 1 });
  await new Promise((r) => setTimeout(r, 80));
  assert.strictEqual(writer.paused, true);
  assert.strictEqual(posts.length, 1, 'the batch is not retried while write access is missing');
  canWrite = true;
  assert.strictEqual(await p, null);
  assert.strictEqual(posts.length, 2);
  await writer.close(100);
});

test('buffer overflow drops the oldest and fails its message', async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const { writer, send } = setup({ maxBufferRows: 2, maxInFlight: 1, flushIntervalMs: 10000 }, async () => { await gate; return ok(); });
  const a = send({ a: 1 });
  const b = send({ a: 2 });
  const c = send({ a: 3 });
  const errA = await a;
  assert.ok(errA);
  assert.match(errA.message, /buffer full/);
  release();
  const closeP = writer.close(500);
  assert.strictEqual(await b, null);
  assert.strictEqual(await c, null);
  await closeP;
});

test('close flushes buffered rows; rows that cannot be sent fail their message', async () => {
  const { writer, posts, send } = setup({ flushIntervalMs: 60000 }, ok);
  const p = send({ a: 1 });
  await writer.close(500);
  assert.strictEqual(await p, null);
  assert.strictEqual(posts.length, 1);

  const s2 = setup({ flushIntervalMs: 60000 }, () => ({ status: 503, headers: { 'x-arc-ingest-fenced': '1', 'retry-after': '60' }, body: Buffer.alloc(0) }));
  const p2 = s2.send({ a: 1 });
  const r = await s2.writer.close(300);
  const err = await p2;
  assert.ok(err);
  assert.match(err.message, /still buffered/);
  assert.strictEqual(r.unsent, 1);
});

test('one request in flight per measurement keeps order', async () => {
  let inFlightM = 0;
  let maxSeen = 0;
  const { writer, send } = setup({ batchSize: 1, maxInFlight: 4 }, async (_, d) => {
    if (d.m === 'm') {
      inFlightM++;
      maxSeen = Math.max(maxSeen, inFlightM);
      await new Promise((r) => setTimeout(r, 5));
      inFlightM--;
    }
    return ok();
  });
  await Promise.all([1, 2, 3, 4, 5].map((i) => send({ a: i })));
  assert.strictEqual(maxSeen, 1);
  await writer.close(100);
});

test('after close returns, a late refusal fails the batch instead of re-buffering it', async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const { writer, send } = setup({ flushIntervalMs: 10 }, async () => {
    await gate;
    return { status: 503, headers: { 'x-arc-ingest-fenced': '1', 'retry-after': '0' }, body: Buffer.alloc(0) };
  });
  const p = send({ a: 1 });
  await new Promise((r) => setTimeout(r, 30));
  const r = await writer.close(50);
  assert.strictEqual(r.inFlight, 1);
  release();
  const err = await p;
  assert.ok(err);
  assert.match(err.message, /node closed/);
  assert.strictEqual(writer.rows, 0);
  assert.strictEqual(writer.retryTimer, null);
  const late = await send({ a: 2 });
  assert.match(late.message, /closed/);
});

test('drop-oldest drops a whole message, never part of it', async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const posts = [];
  const { writer, send } = setup({ maxBufferRows: 3, maxInFlight: 1, flushIntervalMs: 10000, batchSize: 100 }, async (_, d) => { posts.push(d); await gate; return ok(); });
  const a = send([{ a: 1 }, { a: 2 }]);
  const b = send({ a: 3 });
  const c = send([{ a: 4 }, { a: 5 }]);
  const errA = await a;
  assert.match(errA.message, /buffer full/);
  release();
  await writer.close(500);
  assert.strictEqual(await b, null);
  assert.strictEqual(await c, null);
  const written = posts.flatMap((p) => p.columns.a);
  assert.ok(!written.includes(1) && !written.includes(2), 'rows of the dropped message were written: ' + written);
});

test('a hot key cannot starve the others with maxInFlight 1', async () => {
  const order = [];
  const { writer, send } = setup({ batchSize: 2, maxInFlight: 1, flushIntervalMs: 10000 }, async (_, d) => { order.push(d.m); await new Promise((r) => setTimeout(r, 2)); return ok(); });
  const ps = [];
  for (let i = 0; i < 10; i++) ps.push(send({ rows: [{ a: i }, { a: i }], measurement: 'hot' }));
  ps.push(send({ rows: [{ b: 1 }, { b: 2 }], measurement: 'cold' }));
  await Promise.all(ps);
  assert.ok(order.indexOf('cold') < order.length - 1, 'cold waited for every hot batch: ' + order.join(','));
  await writer.close(100);
});

test('a message failing after some of its batches were stored reports how many', async () => {
  let n = 0;
  const { writer, send } = setup({ batchSize: 2, maxInFlight: 1 }, () => (++n === 1 ? ok() : { status: 500, headers: {}, body: Buffer.from('{"error":"x"}') }));
  const err = await send([{ a: 1 }, { a: 2 }, { a: 3 }, { a: 4 }]);
  assert.ok(err);
  assert.strictEqual(err.written, 2);
  await writer.close(100);
});
