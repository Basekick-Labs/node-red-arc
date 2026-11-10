'use strict';

// End-to-end checks: a real Node-RED with this package installed, writing to
// real Arc servers. Usage: node e2e/run.js <node-red-url> <label>
// Targets come from E2E_TARGETS (JSON), set by e2e/run.sh.

const { execSync } = require('child_process');
const buildFlows = require('./flows');

const nrUrl = process.argv[2];
const label = process.argv[3] || nrUrl;
const targets = JSON.parse(process.env.E2E_TARGETS);
const token = process.env.E2E_TOKEN;

let failures = 0;
const results = [];
function check(name, cond, detail) {
  results.push({ name, ok: !!cond, detail });
  if (!cond) failures++;
  console.log(`${cond ? 'PASS' : 'FAIL'} [${label}] ${name}${detail !== undefined ? ' — ' + JSON.stringify(detail) : ''}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function nr(path, body, timeoutMs) {
  const t0 = Date.now();
  const res = await fetch(nrUrl + path, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs || 60000),
  });
  const text = await res.text();
  let json;
  try { json = text ? JSON.parse(text) : null; } catch (_) { json = text; }
  return { status: res.status, body: json, ms: Date.now() - t0 };
}

async function arc(t, path, body, db) {
  const headers = { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' };
  if (db) headers['x-arc-database'] = db;
  const res = await fetch(t.hostUrl + path, { method: 'POST', headers, body: body ? JSON.stringify(body) : undefined });
  const text = await res.text();
  try { return { status: res.status, body: JSON.parse(text) }; } catch (_) { return { status: res.status, body: text }; }
}

// Make Arc write its buffer to Parquet, so the next write lands in a new file.
const flushArc = (t) => arc(t, '/api/v1/write/line-protocol/flush');

async function sql(t, q) {
  const r = await arc(t, '/api/v1/query', { sql: q }, t.db);
  if (r.status !== 200 || !r.body.success) return { error: r.body && (r.body.error || r.body), rows: [] };
  return { rows: r.body.data.map((row) => Object.fromEntries(r.body.columns.map((c, i) => [c, row[i]]))) };
}

async function waitCount(t, measurement, expected, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last = null;
  while (Date.now() < deadline) {
    await flushArc(t);
    const r = await sql(t, `SELECT count(*) AS n, count(DISTINCT seq) AS d FROM ${measurement}`);
    last = r.rows[0] || r.error;
    if (r.rows[0] && Number(r.rows[0].n) >= expected) return r.rows[0];
    await sleep(500);
  }
  return last;
}

async function deploy() {
  const flows = buildFlows(targets.map((t) => ({ name: t.name, url: t.nrArcUrl, token, db: t.db })));
  const res = await fetch(nrUrl + '/flows', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Node-RED-Deployment-Type': 'full' },
    body: JSON.stringify(flows),
  });
  if (res.status !== 204 && res.status !== 200) throw new Error('deploy failed: ' + res.status + ' ' + (await res.text()));
  await sleep(1000);
}

async function runTarget(t) {
  const p = `/${t.name}`;

  // 1. A float column whose first value is integer-valued stays DOUBLE and keeps decimals.
  let r = await nr(`${p}/write?m=typing`, { pressure: 21, sensor: 'a1' });
  check(`${t.name}: write answers 204 after Arc stored it`, r.status === 204, { status: r.status, ms: r.ms, body: r.body });
  await flushArc(t);
  r = await nr(`${p}/write?m=typing`, { pressure: 21.7, sensor: 'a1' });
  await flushArc(t);
  // 2. A batch where the column is null everywhere does not create a VARCHAR file.
  r = await nr(`${p}/write?m=typing`, { pressure: null, sensor: 'a2' });
  await flushArc(t);
  let q = await sql(t, 'SELECT typeof(pressure) AS ty, pressure, sensor FROM typing ORDER BY time');
  check(`${t.name}: pressure is DOUBLE across files`, q.rows.length === 3 && q.rows.every((x) => x.ty === 'DOUBLE'), q.rows.length ? q.rows : q.error);
  check(`${t.name}: 21.7 kept its decimals`, q.rows.some((x) => x.pressure === 21.7), q.rows.map((x) => x.pressure));
  check(`${t.name}: all-null batch stored as NULL`, q.rows.some((x) => x.sensor === 'a2' && x.pressure === null), q.rows);

  // 3. Declared integer column.
  r = await nr(`${p}/write?m=ints`, { count: 5, ratio: 5 });
  await flushArc(t);
  q = await sql(t, 'SELECT typeof("count") AS tc, typeof(ratio) AS tr, "count" AS c FROM ints');
  check(`${t.name}: integerColumns → BIGINT, others DOUBLE`, q.rows[0] && q.rows[0].tc === 'BIGINT' && q.rows[0].tr === 'DOUBLE' && q.rows[0].c === 5, q.rows[0] || q.error);

  // 4. Microsecond timestamps survive the round trip.
  r = await nr(`${p}/write?m=times`, { time: '2026-11-09T10:00:00.123456Z', v: 1 });
  await flushArc(t);
  q = await sql(t, 'SELECT time FROM times');
  check(`${t.name}: µs timestamp exact`, q.rows[0] && String(q.rows[0].time).includes('10:00:00.123456'), q.rows[0] || q.error);

  // 5. Columns-shape payload, mixed types per column across rows.
  r = await nr(`${p}/write?m=cols`, { columns: { a: [1, 2, 3], b: ['x', null, 'z'], ok: [true, false, true] } });
  await flushArc(t);
  q = await sql(t, 'SELECT count(*) AS n, count(b) AS nb, sum(CASE WHEN ok THEN 1 ELSE 0 END) AS nok FROM cols');
  check(`${t.name}: columns payload`, q.rows[0] && Number(q.rows[0].n) === 3 && Number(q.rows[0].nb) === 2 && Number(q.rows[0].nok) === 2, q.rows[0] || q.error);

  // 6. Invalid names are rejected before sending.
  r = await nr(`${p}/write?m=bad%20name`, { v: 1 });
  check(`${t.name}: invalid measurement → Catch, 500`, r.status === 500 && /invalid measurement/.test(JSON.stringify(r.body)), r.body);
  r = await nr(`${p}/write?m=typing`, { _hidden: 1 });
  check(`${t.name}: "_" column rejected`, r.status === 500, r.body);

  // 7. Type mismatch: the cell is left empty, the row is kept.
  r = await nr(`${p}/write?m=typing`, { pressure: 'N/A', sensor: 'a3' });
  check(`${t.name}: type mismatch still writes the row`, r.status === 204, r.body);
  await flushArc(t);
  q = await sql(t, "SELECT pressure FROM typing WHERE sensor = 'a3'");
  check(`${t.name}: mismatched cell stored as NULL`, q.rows.length === 1 && q.rows[0].pressure === null, q.rows);

  // 8. arc-query with quoted params.
  r = await nr(`${p}/query`, { sql: 'SELECT count(*) AS n FROM typing WHERE sensor = {{s}}', params: { s: "a1' OR '1'='1" } });
  check(`${t.name}: query param is quoted (injection returns 0 rows)`, r.status === 200 && r.body[0] && Number(r.body[0].n) === 0, r.body);
  r = await nr(`${p}/query`, { sql: 'SELECT count(*) AS n FROM typing WHERE sensor = {{s}}', params: { s: 'a1' } });
  check(`${t.name}: query returns row objects`, r.status === 200 && r.body[0] && Number(r.body[0].n) === 2, r.body);

  // 9. Burst: every row arrives exactly once.
  const N = 20000;
  const t0 = Date.now();
  r = await nr(`${p}/burst`, { m: 'burst', n: N });
  const got = await waitCount(t, 'burst', N, 60000);
  const secs = (Date.now() - t0) / 1000;
  check(`${t.name}: burst ${N} rows, each exactly once`, got && Number(got.n) === N && Number(got.d) === N, { got, secs });

  // 10. Redeploy with rows still buffered: the close handler flushes them.
  r = await nr(`${p}/slow`, { m: 'redeploy', n: 500 });
  await sleep(500);
  await flushArc(t);
  let before = await sql(t, 'SELECT count(*) AS n FROM redeploy');
  const beforeN = before.rows[0] ? Number(before.rows[0].n) : 0;
  await deploy();
  const after = await waitCount(t, 'redeploy', 500, 15000);
  check(`${t.name}: redeploy flushes buffered rows`, beforeN === 0 && after && Number(after.n) === 500 && Number(after.d) === 500, { beforeN, after });
}

// Arc outage: the write is held while Arc is down (connection refused is
// resent with backoff) and completes once Arc is back.
async function runOutage(t) {
  const p = `/${t.name}`;
  execSync(t.stop, { stdio: 'ignore' });
  const pending = nr(`${p}/write?m=outage`, { seq: 1, v: 1 }, 120000);
  await sleep(4000);
  execSync(t.start, { stdio: 'ignore' });
  const r = await pending;
  check(`${t.name}: write during an Arc outage completes after Arc returns`, r.status === 204, { status: r.status, ms: r.ms, body: r.body });
  const got = await waitCount(t, 'outage', 1, 20000);
  check(`${t.name}: outage row stored exactly once`, got && Number(got.n) === 1, got);
}

(async () => {
  await deploy();
  for (const t of targets) await runTarget(t);
  for (const t of targets) if (t.stop) await runOutage(t);
  console.log(`[${label}] ${results.length - failures}/${results.length} passed`);
  process.exit(failures ? 1 : 0);
})().catch((err) => {
  console.error(err);
  process.exit(2);
});
