'use strict';

// Small HTTP client on node:http(s). Global fetch is not used: it takes no
// agent, so the core tls-config node's options could not be applied.

const http = require('http');
const https = require('https');
const zlib = require('zlib');

function createAgent(url, tlsOptions) {
  const isHttps = new URL(url).protocol === 'https:';
  const opts = Object.assign({ keepAlive: true, maxSockets: 8, timeout: 30000 }, isHttps ? tlsOptions || {} : {});
  return isHttps ? new https.Agent(opts) : new http.Agent(opts);
}

// Join the base URL (which may carry a path prefix for a reverse proxy) with an
// API path.
function joinUrl(base, path) {
  const u = new URL(base);
  const prefix = u.pathname.replace(/\/+$/, '');
  u.pathname = prefix + path;
  u.search = '';
  return u;
}

// One request. Resolves {status, headers, body: Buffer}. Rejects with an Error
// carrying .bodySent (the request body was fully written), .reusedSocket and
// .gotResponse. A reset on a reused keep-alive socket before any response byte
// is the server closing an idle connection; that request is retried once on a
// fresh socket.
function request(opts) {
  return once(opts).catch((err) => {
    if (err.reusedSocket && !err.gotResponse && (err.code === 'ECONNRESET' || err.code === 'EPIPE')) {
      return once(Object.assign({}, opts, { freshSocket: true }));
    }
    throw err;
  });
}

function once(opts) {
  return new Promise((resolve, reject) => {
    const url = joinUrl(opts.baseUrl, opts.path);
    const lib = url.protocol === 'https:' ? https : http;
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      reject(new Error(`unsupported URL scheme ${url.protocol}`));
      return;
    }
    const headers = Object.assign({}, opts.headers);
    if (opts.body) headers['Content-Length'] = String(opts.body.length);
    const reqOpts = {
      method: opts.method || 'GET',
      headers,
      timeout: opts.timeoutMs || 30000,
      agent: opts.freshSocket ? undefined : opts.agent,
    };
    if (opts.freshSocket && opts.tlsOptions) Object.assign(reqOpts, opts.tlsOptions);
    let bodySent = false;
    let gotResponse = false;
    let settled = false;
    const fail = (err) => {
      if (settled) return;
      settled = true;
      err.bodySent = bodySent;
      err.gotResponse = gotResponse;
      err.reusedSocket = !!req.reusedSocket;
      reject(err);
    };
    const req = lib.request(url, reqOpts, (res) => {
      gotResponse = true;
      const chunks = [];
      let size = 0;
      const limit = opts.maxResponseBytes || 64 * 1024 * 1024;
      let stream = res;
      const enc = (res.headers['content-encoding'] || '').toLowerCase();
      if (enc === 'gzip') stream = res.pipe(zlib.createGunzip());
      else if (enc === 'deflate') stream = res.pipe(zlib.createInflate());
      stream.on('data', (c) => {
        size += c.length;
        if (size > limit) {
          req.destroy();
          fail(Object.assign(new Error(`response larger than ${limit} bytes`), { code: 'ERESPONSETOOLARGE' }));
          return;
        }
        chunks.push(c);
      });
      stream.on('end', () => {
        if (settled) return;
        settled = true;
        resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) });
      });
      stream.on('error', fail);
      res.on('error', fail);
    });
    req.on('timeout', () => {
      req.destroy(Object.assign(new Error('request timed out'), { code: 'ETIMEDOUT' }));
    });
    req.on('error', (err) => {
      if (err.code && /^(ERR_TLS|ERR_SSL|CERT_|UNABLE_TO|DEPTH_ZERO|SELF_SIGNED|ERR_OSSL)/.test(err.code) && !bodySent) {
        err.tlsHandshake = true;
      }
      fail(err);
    });
    req.on('finish', () => {
      bodySent = true;
    });
    req.end(opts.body);
  });
}

// gzip above 1 KiB; Arc detects compression from the magic bytes.
function compress(body, mode) {
  if (mode === 'none' || body.length < 1024) return body;
  if (mode === 'zstd' || (mode === 'auto' && typeof zlib.zstdCompressSync === 'function')) {
    if (typeof zlib.zstdCompressSync === 'function') return zlib.zstdCompressSync(body);
  }
  return zlib.gzipSync(body, { level: 1 });
}

module.exports = { createAgent, request, compress, joinUrl };
