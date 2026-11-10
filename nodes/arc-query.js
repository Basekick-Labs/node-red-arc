'use strict';

const { checkDatabase } = require('../lib/names');
const { retryAfterMs } = require('../lib/retry');

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

// Arc has no query parameters, so values are quoted here. {{name}} takes
// msg.params.name as a literal; {{#name}} as an identifier (checked, not quoted).
function render(sql, params) {
  return sql.replace(/\{\{\s*(#?)([A-Za-z_][A-Za-z0-9_]*)\s*\}\}/g, (_, ident, name) => {
    if (!params || !(name in params)) throw new Error(`missing msg.params.${name}`);
    const v = params[name];
    if (ident) {
      if (typeof v !== 'string' || !IDENT_RE.test(v)) throw new Error(`msg.params.${name} is not a valid identifier`);
      return '"' + v + '"';
    }
    return literal(v, name);
  });
}

function literal(v, name) {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) throw new Error(`msg.params.${name} is not a finite number`);
    return String(v);
  }
  if (typeof v === 'bigint') return v.toString();
  if (typeof v === 'boolean') return v ? 'TRUE' : 'FALSE';
  if (v instanceof Date) return "TIMESTAMP '" + v.toISOString().replace('T', ' ').replace('Z', '') + "'";
  if (typeof v === 'string') return "'" + v.replace(/'/g, "''") + "'";
  if (Array.isArray(v)) return '(' + v.map((x) => literal(x, name)).join(', ') + ')';
  throw new Error(`msg.params.${name} has unsupported type ${typeof v}`);
}

// Parse JSON keeping integers beyond 2^53 as strings where the runtime exposes
// the source text to the reviver (Node 21+). Older runtimes lose precision.
function parseJson(text) {
  return JSON.parse(text, function (key, value, context) {
    if (typeof value === 'number' && !Number.isSafeInteger(value) && Number.isInteger(value) && context && context.source) {
      return context.source;
    }
    return value;
  });
}

function convertTime(v, mode) {
  if (typeof v !== 'string' || mode === 'string') return v;
  if (mode === 'date') {
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? v : d;
  }
  if (mode === 'epochUs') {
    const m = /^(.*?T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})?$/.exec(v);
    if (!m) return v;
    const base = Date.parse(m[1] + (m[3] || 'Z'));
    if (Number.isNaN(base)) return v;
    const frac = (m[2] || '').slice(0, 6).padEnd(6, '0');
    return (BigInt(base) * 1000n + BigInt(frac)).toString();
  }
  return v;
}

module.exports = function (RED) {
  function ArcQueryNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const server = RED.nodes.getNode(config.server);
    const output = config.output || 'rows';
    const rowsPerMsg = Math.max(1, Number(config.rowsPerMsg) || 1000);
    const timeFormat = config.timeFormat || 'string';
    const maxResponseBytes = (Number(config.maxResponseMB) || 64) * 1024 * 1024;

    node.on('input', async (msg, send, done) => {
      if (!server) {
        done(new Error('arc-query: no Arc server configured'));
        return;
      }
      let sql = config.sql;
      if (config.allowMsgSql && typeof msg.sql === 'string' && msg.sql.trim()) sql = msg.sql;
      if (!sql || !sql.trim()) {
        done(new Error('arc-query: no SQL (set it on the node, or enable msg.sql)'));
        return;
      }
      try {
        sql = render(sql, msg.params);
      } catch (err) {
        done(err);
        return;
      }
      const database = msg.database || config.database || server.database || '';
      const headers = { 'Content-Type': 'application/json', 'Accept-Encoding': 'gzip' };
      if (database) {
        const dbErr = checkDatabase(database);
        if (dbErr) {
          done(new Error(dbErr));
          return;
        }
        headers['x-arc-database'] = database;
      }
      node.status({ fill: 'blue', shape: 'dot', text: 'querying' });
      const call = () => server.request({
        method: 'POST', path: '/api/v1/query', headers, body: Buffer.from(JSON.stringify({ sql })), maxResponseBytes,
      });
      let res;
      try {
        res = await call();
        if (res.status === 503) {
          const wait = Math.min(retryAfterMs(res.headers) || 1000, 10000);
          await new Promise((r) => setTimeout(r, wait));
          res = await call();
        }
      } catch (err) {
        node.status({ fill: 'red', shape: 'dot', text: err.code || 'error' });
        msg.arc = { error: err.message };
        done(err);
        return;
      }
      let body;
      try {
        body = parseJson(res.body.toString('utf8'));
      } catch (_) {
        body = null;
      }
      if (res.status !== 200 || !body || body.success === false) {
        const text = (body && body.error) || res.body.toString('utf8').slice(0, 300);
        const ra = retryAfterMs(res.headers);
        msg.arc = { status: res.status, error: text, retryAfterMs: ra };
        node.status({ fill: res.status === 429 ? 'yellow' : 'red', shape: 'dot', text: `${res.status}` });
        done(new Error(`Arc query failed (${res.status}): ${text}`));
        return;
      }
      const columns = body.columns || [];
      const data = body.data || [];
      const timeIdx = [];
      columns.forEach((c, i) => {
        if (String(c).toLowerCase() === 'time') timeIdx.push(i);
      });
      if (timeFormat !== 'string' && timeIdx.length) {
        for (const row of data) for (const i of timeIdx) row[i] = convertTime(row[i], timeFormat);
      }
      const toObjects = (rows) => rows.map((r) => {
        const o = {};
        columns.forEach((c, i) => {
          o[c] = r[i];
        });
        return o;
      });
      msg.arc = {
        rowCount: body.row_count !== undefined ? body.row_count : data.length,
        executionTimeMs: body.execution_time_ms,
        truncated: !!body.truncated,
        truncationReason: body.truncation_reason,
        rowsCapped: !!body.rows_capped,
        rowCap: body.row_cap,
        columns,
      };
      const capped = msg.arc.truncated || msg.arc.rowsCapped;
      node.status({ fill: capped ? 'yellow' : 'green', shape: 'dot', text: `${data.length} rows${capped ? ' (capped)' : ''}` });
      if (output === 'raw') {
        msg.payload = { columns, data };
        send(msg);
      } else if (output === 'single') {
        const objs = toObjects(data);
        objs.forEach((o, i) => send(Object.assign(RED.util.cloneMessage(msg), { payload: o, parts: { id: msg._msgid, index: i, count: objs.length } })));
      } else if (output === 'chunks') {
        const count = Math.ceil(data.length / rowsPerMsg) || 1;
        for (let i = 0; i < count; i++) {
          const chunk = toObjects(data.slice(i * rowsPerMsg, (i + 1) * rowsPerMsg));
          send(Object.assign(RED.util.cloneMessage(msg), { payload: chunk, parts: { id: msg._msgid, index: i, count } }));
        }
      } else {
        msg.payload = toObjects(data);
        send(msg);
      }
      done();
    });
  }

  RED.nodes.registerType('arc-query', ArcQueryNode);
};

module.exports.render = render;
module.exports.convertTime = convertTime;
