'use strict';

const { createAgent, request, joinUrl } = require('../lib/http');

const PASSWORD_PLACEHOLDER = '__PWRD__';

module.exports = function (RED) {
  function ArcServerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.url = (config.url || '').trim();
    node.database = config.database || 'default';
    node.timeoutMs = Number(config.timeoutMs) || 30000;
    node.compression = config.compression || 'auto';
    // The tls-config node is looked up on first use, not here: Node-RED does
    // not guarantee it is constructed before this node.
    node.tlsOptions = {};
    let tlsResolved = !config.tls;
    const resolveTls = () => {
      if (tlsResolved) return;
      const tlsNode = RED.nodes.getNode(config.tls);
      if (!tlsNode || typeof tlsNode.addTLSOptions !== 'function') {
        throw new Error(`TLS configuration ${config.tls} of this Arc server is not available`);
      }
      tlsNode.addTLSOptions(node.tlsOptions);
      tlsResolved = true;
    };

    // The agent is shared by every node using this config. Node-RED closes
    // config nodes and the nodes using them concurrently on a redeploy, so the
    // agent is destroyed only after the last user has finished its final flush.
    let agent = null;
    let users = 0;
    let closing = false;
    // Once closing, no new pooled agent is created: a late request (a node
    // still finishing its flush) gets an unpooled socket instead of a pool
    // nobody would destroy.
    node.agent = () => {
      resolveTls();
      if (!agent) {
        if (closing) return false;
        agent = createAgent(node.url, node.tlsOptions);
      }
      return agent;
    };
    node.register = () => {
      users++;
    };
    node.deregister = () => {
      users--;
      if (users <= 0 && closing && agent) {
        agent.destroy();
        agent = null;
      }
    };

    node.headers = (extra) => {
      const h = Object.assign({ 'User-Agent': 'node-red-arc' }, extra);
      const token = node.credentials && node.credentials.token;
      if (token) h.Authorization = 'Bearer ' + token;
      return h;
    };

    node.request = (opts) => {
      let agentNow;
      try {
        agentNow = node.agent();
      } catch (err) {
        return Promise.reject(err);
      }
      return request(Object.assign({
        baseUrl: node.url,
        agent: agentNow,
        tlsOptions: node.tlsOptions,
        timeoutMs: node.timeoutMs,
      }, opts, { headers: node.headers(opts.headers) }));
    };

    // True when the token is accepted (or the server has auth disabled).
    node.verifyAuth = async () => {
      const res = await node.request({ method: 'GET', path: '/api/v1/auth/verify' });
      return res.status === 200 || res.status === 404;
    };

    node.on('close', (done) => {
      closing = true;
      if (users <= 0 && agent) {
        agent.destroy();
        agent = null;
      }
      done();
    });
  }

  RED.nodes.registerType('arc-server', ArcServerNode, {
    credentials: { token: { type: 'password' } },
  });

  // Editor "Test connection". A stored token is only ever sent to the URL it
  // was stored with: a changed URL requires the token to be typed again, so an
  // editor user cannot make Node-RED send a deployed token to another host.
  RED.httpAdmin.post('/arc-server/test', RED.auth.needsPermission('arc-server.write'), async (req, res) => {
    const body = req.body || {};
    // The editor sends field values as typed, so ${VAR} references are
    // resolved here the way the runtime resolves them on deploy.
    const env = (v) => String(v || '').replace(/\$\{(\w+)\}/g, (_, k) => (process.env[k] !== undefined ? process.env[k] : ''));
    const url = env(body.url).trim();
    let parsed;
    try {
      parsed = new URL(url);
    } catch (_) {
      res.json({ ok: false, message: 'Invalid URL' });
      return;
    }
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      res.json({ ok: false, message: 'URL must be http or https' });
      return;
    }
    let token = typeof body.token === 'string' ? body.token : '';
    if (token === PASSWORD_PLACEHOLDER || token === '') {
      token = '';
      if (body.id) {
        const deployed = RED.nodes.getNode(body.id);
        const creds = RED.nodes.getCredentials(body.id);
        if (creds && creds.token) {
          if (!deployed || deployed.url !== url) {
            res.json({ ok: false, message: 'The URL changed since the token was saved (or the node is not deployed); enter the token again to test it against this URL' });
            return;
          }
          // The deployed node holds the token with ${VAR} already resolved.
          token = (deployed.credentials && deployed.credentials.token) || env(creds.token);
        }
      }
    } else {
      token = env(token);
    }
    const tlsOptions = {};
    if (body.tls) {
      const tlsNode = RED.nodes.getNode(body.tls);
      if (tlsNode && typeof tlsNode.addTLSOptions === 'function') tlsNode.addTLSOptions(tlsOptions);
    }
    const agent = createAgent(url, tlsOptions);
    const headers = { 'User-Agent': 'node-red-arc' };
    if (token) headers.Authorization = 'Bearer ' + token;
    const call = (path) => request({ baseUrl: url, path, method: 'GET', agent, headers, timeoutMs: 5000, maxResponseBytes: 1024 * 1024 });
    try {
      const health = await call('/health');
      if (health.status !== 200) {
        res.json({ ok: false, message: `${joinUrl(url, '/health')} answered ${health.status}` });
        return;
      }
      const verify = await call('/api/v1/auth/verify');
      if (verify.status === 404) {
        res.json({ ok: true, message: 'Connected. Authentication is disabled on this Arc server.' });
        return;
      }
      if (verify.status !== 200) {
        res.json({ ok: false, message: token ? 'Connected, but the token was rejected' : 'Connected, but the server requires a token' });
        return;
      }
      let perms = [];
      try {
        const info = JSON.parse(verify.body.toString('utf8'));
        perms = info.permissions || (info.token_info && info.token_info.permissions) || [];
      } catch (_) {
        // keep empty
      }
      const list = Array.isArray(perms) ? perms : String(perms).split(',');
      const has = (p) => list.includes(p) || list.includes('admin');
      const missing = ['write', 'read'].filter((p) => !has(p));
      res.json({
        ok: true,
        message: `Connected. Token permissions: ${list.join(', ') || 'unknown'}` +
          (missing.length ? ` (missing ${missing.join(' and ')})` : ''),
      });
    } catch (err) {
      res.json({ ok: false, message: `Cannot reach ${url}: ${err.code || err.message}` });
    } finally {
      agent.destroy();
    }
  });
};
