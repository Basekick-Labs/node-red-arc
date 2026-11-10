'use strict';

const { RowTyper, RowError } = require('../lib/rows');
const { Writer } = require('../lib/writer');
const { compress } = require('../lib/http');

function list(s) {
  return String(s || '').split(',').map((x) => x.trim()).filter(Boolean);
}

function num(v, fallback) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

// Values on a msg that Debug and JSON context storage cannot serialise.
function bigintToString(value) {
  if (typeof value === 'bigint') return value.toString();
  if (Array.isArray(value)) return value.map(bigintToString);
  if (value && typeof value === 'object' && !(value instanceof Date) && !Buffer.isBuffer(value)) {
    const out = {};
    for (const k of Object.keys(value)) out[k] = bigintToString(value[k]);
    return out;
  }
  return value;
}

module.exports = function (RED) {
  function ArcWriteNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const server = RED.nodes.getNode(config.server);
    if (!server) {
      node.status({ fill: 'red', shape: 'ring', text: 'no server configured' });
      node.on('input', (msg, send, done) => done(new Error('arc-write: no Arc server configured')));
      return;
    }
    server.register();

    const typer = new RowTyper({
      integerColumns: list(config.integerColumns),
      onTypeMismatch: config.onTypeMismatch || 'null',
      stringifyObjects: !!config.stringifyObjects,
      sanitizeNames: !!config.sanitizeNames,
      precision: config.precision || 'ms',
    });
    const defaults = { database: config.database || server.database, measurement: config.measurement };
    const resultsOutput = Number(config.outputs) > 0;

    let lastStatus = 0;
    let statusTimer = null;
    let latest = null;
    const showStatus = () => {
      statusTimer = null;
      lastStatus = Date.now();
      const s = latest;
      if (!s) return;
      if (s.state === 'auth') node.status({ fill: 'red', shape: 'ring', text: 'auth failed, paused' });
      else if (s.state === 'retrying') node.status({ fill: 'yellow', shape: 'ring', text: `${s.text} · ${s.buffered} buffered` });
      else if (s.state === 'error') node.status({ fill: 'red', shape: 'dot', text: String(s.text).slice(0, 60) });
      else node.status({ fill: 'green', shape: 'dot', text: `${s.buffered} buffered` });
    };

    // Rate-limit log lines: one per reason per 10 s.
    const logged = new Map();
    const logOnce = (reason, fn) => {
      const now = Date.now();
      if ((logged.get(reason) || 0) + 10000 > now) return;
      logged.set(reason, now);
      fn();
    };

    const compression = server.compression;
    const writer = new Writer({
      batchSize: num(config.batchSize, 5000),
      flushIntervalMs: num(config.flushInterval, 1000),
      maxBufferRows: num(config.maxBufferRows, 200000),
      maxBufferBytes: num(config.maxBufferMB, 64) * 1024 * 1024,
      maxPendingMessages: num(config.maxPendingMessages, 100000),
      maxInFlight: num(config.maxInFlight, 4),
      overflow: config.overflow || 'drop-oldest',
      compress: (body) => compress(body, compression),
      post: (db, body) => server.request({
        method: 'POST',
        path: '/api/v1/write/msgpack',
        headers: { 'Content-Type': 'application/msgpack', 'x-arc-database': db },
        body,
      }),
      authProbe: () => server.verifyAuth(),
      onStatus: (s) => {
        latest = s;
        if (statusTimer) return;
        const wait = Math.max(0, lastStatus + 1000 - Date.now());
        statusTimer = setTimeout(showStatus, wait);
      },
      onBatch: (info) => {
        if (info.action !== 'ok' && info.action !== 'split') {
          logOnce(info.reason, () => node.warn(`Arc ${info.database}/${info.measurement}: ${info.reason}`));
        }
        if (resultsOutput) node.send({ topic: info.measurement, payload: info });
      },
    });

    node.on('input', (msg, send, done) => {
      if (msg.flush === true && msg.payload === undefined) {
        writer.flush();
        done();
        return;
      }
      let typed;
      try {
        typed = typer.type(msg, defaults);
      } catch (err) {
        if (err instanceof RowError) {
          msg.arc = { reason: 'rejected', error: err.message };
          done(err);
          return;
        }
        throw err;
      }
      if (typed.warnings.length) {
        msg.arc = Object.assign({}, msg.arc, { rejectedCells: bigintToString(typed.warnings) });
        logOnce('mismatch:' + typed.warnings[0].column, () => node.warn(typed.warnings[0].message + ' (cell left empty)'));
      }
      const handle = writer.handle((err) => {
        if (err) {
          msg.arc = Object.assign({}, msg.arc, bigintToString(err.arc || {}));
          done(err);
        } else {
          done();
        }
      });
      writer.enqueue(typed.groups, handle);
      if (msg.flush === true) writer.flush();
    });

    node.on('close', async (removed, done) => {
      const budget = Math.min(10000, Math.max(1000, (Number(RED.settings.nodeCloseTimeout) || 15000) - 2000));
      try {
        const r = await writer.close(budget);
        if (r.unsent) node.warn(`closed with ${r.unsent} rows not written to Arc (reported to Catch nodes)`);
      } finally {
        if (statusTimer) clearTimeout(statusTimer);
        server.deregister();
        done();
      }
    });
  }

  RED.nodes.registerType('arc-write', ArcWriteNode);
};
