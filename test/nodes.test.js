'use strict';

const test = require('node:test');
const assert = require('node:assert');
const http = require('http');
const helper = require('node-red-node-test-helper');
const { decode } = require('@msgpack/msgpack');
const zlib = require('zlib');
const serverNode = require('../nodes/arc-server.js');
const writeNode = require('../nodes/arc-write.js');
const queryNode = require('../nodes/arc-query.js');

helper.init(require.resolve('node-red'));

// A fake Arc: records writes, answers queries, and can be told what to return.
function fakeArc() {
  const state = { writes: [], auth: [], queries: [], writeStatus: 204, seenTokens: [] };
  const srv = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      let body = Buffer.concat(chunks);
      state.seenTokens.push(req.headers.authorization || '');
      if (req.url === '/health') {
        res.end('{"status":"ok"}');
        return;
      }
      if (req.url === '/api/v1/auth/verify') {
        const ok = req.headers.authorization === 'Bearer good';
        res.statusCode = ok ? 200 : 401;
        res.end(JSON.stringify(ok ? { valid: true, permissions: ['read', 'write'] } : { valid: false }));
        return;
      }
      if (req.url === '/api/v1/write/msgpack') {
        if (body[0] === 0x1f && body[1] === 0x8b) body = zlib.gunzipSync(body);
        state.writes.push({ db: req.headers['x-arc-database'], payload: decode(body, { useBigInt64: true }) });
        res.statusCode = state.writeStatus;
        res.end();
        return;
      }
      if (req.url === '/api/v1/query') {
        const q = JSON.parse(body.toString());
        state.queries.push({ sql: q.sql, db: req.headers['x-arc-database'] });
        res.setHeader('Content-Type', 'application/json');
        res.end('{"success":true,"columns":["time","v","big"],"data":[["2026-11-09T10:00:00.123456Z",1.5,9007199254740993]],"row_count":1,"execution_time_ms":1}');
        return;
      }
      res.statusCode = 404;
      res.end();
    });
  });
  return new Promise((resolve) => srv.listen(0, '127.0.0.1', () => resolve({ srv, state, url: `http://127.0.0.1:${srv.address().port}` })));
}

function flow(url, writeCfg) {
  return [
    { id: 's1', type: 'arc-server', url, database: 'plant' },
    Object.assign({ id: 'w1', type: 'arc-write', server: 's1', measurement: 'boiler', flushInterval: 50, wires: [] }, writeCfg),
    { id: 'c1', type: 'complete', scope: ['w1'], wires: [['h1']] },
    { id: 'k1', type: 'catch', scope: ['w1'], uncaught: false, wires: [['h2']] },
    { id: 'h1', type: 'helper' },
    { id: 'h2', type: 'helper' },
  ];
}

const nodes = [serverNode, writeNode, queryNode, require('@node-red/nodes/core/common/24-complete.js'), require('@node-red/nodes/core/common/25-catch.js')];

test.afterEach(async () => {
  await helper.unload();
});

test('arc-write: Complete fires after the 204, and the body is typed msgpack', async () => {
  const arc = await fakeArc();
  await helper.load(nodes, flow(arc.url), { s1: { token: 'good' } });
  const done = new Promise((resolve) => helper.getNode('h1').on('input', resolve));
  helper.getNode('w1').receive({ payload: { temperature: 21, sensor: 'a1' } });
  const msg = await done;
  assert.deepStrictEqual(msg.payload, { temperature: 21, sensor: 'a1' });
  assert.strictEqual(arc.state.writes.length, 1);
  assert.strictEqual(arc.state.writes[0].db, 'plant');
  assert.strictEqual(arc.state.writes[0].payload.m, 'boiler');
  assert.ok(arc.seenTokenOk !== false && arc.state.seenTokens.includes('Bearer good'));
  arc.srv.close();
});

test('arc-write: an unmarked 503 reaches Catch with msg.arc and is not resent', async () => {
  const arc = await fakeArc();
  arc.state.writeStatus = 503;
  await helper.load(nodes, flow(arc.url), { s1: { token: 'good' } });
  const caught = new Promise((resolve) => helper.getNode('h2').on('input', resolve));
  helper.getNode('w1').receive({ payload: { v: 1 } });
  const msg = await caught;
  assert.strictEqual(msg.arc.status, 503);
  assert.match(msg.error.message, /not resent/);
  await new Promise((r) => setTimeout(r, 200));
  assert.strictEqual(arc.state.writes.length, 1);
  arc.srv.close();
});

test('arc-write: invalid measurement is rejected immediately and never sent', async () => {
  const arc = await fakeArc();
  await helper.load(nodes, flow(arc.url), { s1: { token: 'good' } });
  const caught = new Promise((resolve) => helper.getNode('h2').on('input', resolve));
  helper.getNode('w1').receive({ payload: { v: 1 }, measurement: 'plant/line 3' });
  const msg = await caught;
  assert.match(msg.error.message, /invalid measurement/);
  assert.strictEqual(arc.state.writes.length, 0);
  arc.srv.close();
});

test('arc-write: stopping the flow flushes buffered rows through the shared agent', async () => {
  const arc = await fakeArc();
  await helper.load(nodes, flow(arc.url, { flushInterval: 60000 }), { s1: { token: 'good' } });
  for (let i = 0; i < 10; i++) helper.getNode('w1').receive({ payload: { v: i } });
  await new Promise((r) => setTimeout(r, 50));
  assert.strictEqual(arc.state.writes.length, 0);
  await helper.unload();
  assert.strictEqual(arc.state.writes.length, 1);
  assert.strictEqual(arc.state.writes[0].payload.columns.v.length, 10);
  arc.srv.close();
});

test('arc-query: rows as objects, params quoted, big ints kept where supported', async () => {
  const arc = await fakeArc();
  await helper.load(nodes, [
    { id: 's1', type: 'arc-server', url: arc.url },
    { id: 'q1', type: 'arc-query', server: 's1', database: 'plant', sql: "SELECT * FROM boiler WHERE sensor = {{s}}", wires: [['h1']] },
    { id: 'h1', type: 'helper' },
  ], { s1: { token: 'good' } });
  const got = new Promise((resolve) => helper.getNode('h1').on('input', resolve));
  helper.getNode('q1').receive({ params: { s: "a1' OR '1'='1" } });
  const msg = await got;
  assert.strictEqual(arc.state.queries[0].sql, "SELECT * FROM boiler WHERE sensor = 'a1'' OR ''1''=''1'");
  assert.strictEqual(arc.state.queries[0].db, 'plant');
  assert.strictEqual(msg.payload[0].time, '2026-11-09T10:00:00.123456Z');
  assert.strictEqual(msg.payload[0].v, 1.5);
  const major = Number(process.versions.node.split('.')[0]);
  if (major >= 21) assert.strictEqual(msg.payload[0].big, '9007199254740993');
  assert.strictEqual(msg.arc.rowCount, 1);
  arc.srv.close();
});

test('test connection: a stored token is never sent to a different URL', async () => {
  const arc = await fakeArc();
  const other = await fakeArc();
  await helper.load(nodes, [{ id: 's1', type: 'arc-server', url: arc.url }], { s1: { token: 'good' } });
  const post = async (body) => (await helper.request().post('/arc-server/test').send(body)).body;
  const same = await post({ id: 's1', url: arc.url, token: '__PWRD__' });
  assert.strictEqual(same.ok, true, same.message);
  const moved = await post({ id: 's1', url: other.url, token: '__PWRD__' });
  assert.strictEqual(moved.ok, false);
  assert.ok(!other.state.seenTokens.includes('Bearer good'), 'stored token leaked to another URL');
  const bad = await post({ id: 's1', url: 'file:///etc/passwd', token: 'x' });
  assert.strictEqual(bad.ok, false);
  arc.srv.close();
  other.srv.close();
});
