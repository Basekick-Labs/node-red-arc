'use strict';

// Builds the Node-RED flow used by the end-to-end run: for each Arc target, an
// HTTP API in front of arc-write / arc-query so the harness can drive them.
//
//   POST /<t>/write?m=<measurement>[&db=<database>]  body = row(s); answers 204 only after the
//                                    Complete node fired (Arc answered 204),
//                                    500 with the error when Catch fired
//   POST /<t>/burst  {m, n}          sends n messages into arc-write, answers at once
//   POST /<t>/slow   {m, n}          same into an arc-write that flushes every 60 s
//   POST /<t>/query  {sql, params}   arc-query, answers the rows

function targetNodes(t, url, token, db, ca) {
  const id = (s) => `${t}_${s}`;
  const tab = id('tab');
  const n = (o) => Object.assign({ z: tab }, o);
  const respond = n({
    id: id('respond'), type: 'function', name: 'respond',
    func: 'if (!msg.res) return null;\nmsg.statusCode = msg.statusCode || 204;\nif (msg.statusCode === 204) msg.payload = "";\nreturn msg;',
    outputs: 1, wires: [[id('httpres')]],
  });
  const errorOut = n({
    id: id('errorOut'), type: 'function', name: 'error',
    func: 'if (!msg.res) return null;\nmsg.statusCode = 500;\nmsg.payload = { error: msg.error && msg.error.message, arc: msg.arc };\nreturn msg;',
    outputs: 1, wires: [[id('httpres')]],
  });
  const burst = (name, writer) => n({
    id: id(name + 'fn'), type: 'function', name,
    func: 'const { m, n } = msg.payload;\nfor (let i = 0; i < n; i++) node.send([{ measurement: m, payload: { seq: i, v: i / 10 } }, null]);\nmsg.payload = { queued: n };\nmsg.statusCode = 200;\nreturn [null, msg];',
    outputs: 2, wires: [[writer], [id('httpres')]],
  });
  return [
    { id: tab, type: 'tab', label: `e2e ${t}` },
    ...(ca ? [{ id: id('tls'), type: 'tls-config', cert: '', key: '', ca: '', servername: '', alpnprotocol: '', certname: '', keyname: '', caname: '', verifyservercert: true, credentials: { cadata: ca } }] : []),
    Object.assign({ id: id('server'), type: 'arc-server', url, database: db, credentials: { token } }, ca ? { tls: id('tls') } : {}),
    n({ id: id('in_write'), type: 'http in', url: `/${t}/write`, method: 'post', wires: [[id('prep')]] }),
    n({
      id: id('prep'), type: 'function', name: 'prep',
      func: 'msg.measurement = msg.req.query.m;\nif (msg.req.query.db) msg.database = msg.req.query.db;\nmsg.payload = msg.req.body;\nreturn msg;',
      outputs: 1, wires: [[id('writer')]],
    }),
    n({
      id: id('writer'), type: 'arc-write', server: id('server'), flushInterval: 200, batchSize: 5000,
      integerColumns: 'count', onTypeMismatch: 'null', maxBufferRows: 200000, wires: [],
    }),
    n({ id: id('in_burst'), type: 'http in', url: `/${t}/burst`, method: 'post', wires: [[id('burstfn')]] }),
    burst('burst', id('writer')),
    n({ id: id('in_slow'), type: 'http in', url: `/${t}/slow`, method: 'post', wires: [[id('slowfn')]] }),
    burst('slow', id('slowwriter')),
    n({ id: id('slowwriter'), type: 'arc-write', server: id('server'), flushInterval: 60000, batchSize: 100000, wires: [] }),
    n({ id: id('complete'), type: 'complete', scope: [id('writer'), id('slowwriter')], wires: [[id('respond')]] }),
    n({ id: id('catch'), type: 'catch', scope: [id('writer'), id('slowwriter'), id('query')], uncaught: false, wires: [[id('errorOut')]] }),
    respond,
    errorOut,
    n({ id: id('in_query'), type: 'http in', url: `/${t}/query`, method: 'post', wires: [[id('qprep')]] }),
    n({
      id: id('qprep'), type: 'function', name: 'qprep',
      func: 'msg.sql = msg.req.body.sql;\nmsg.params = msg.req.body.params;\nfor (const k of Object.keys(msg.params || {})) if (k.endsWith("_date")) msg.params[k] = new Date(msg.params[k]);\nreturn msg;',
      outputs: 1, wires: [[id('query')]],
    }),
    n({ id: id('query'), type: 'arc-query', server: id('server'), allowMsgSql: true, output: 'rows', timeFormat: 'string', wires: [[id('qres')]] }),
    n({ id: id('qres'), type: 'function', name: 'qres', func: 'msg.statusCode = 200;\nreturn msg;', outputs: 1, wires: [[id('httpres')]] }),
    n({ id: id('httpres'), type: 'http response', statusCode: '', headers: {} }),
  ];
}

module.exports = function buildFlows(targets) {
  return targets.flatMap((t) => targetNodes(t.name, t.url, t.token, t.db, t.ca));
};
