'use strict';

// Decides what to do with Arc's answer to a write. The rule: resend only what
// Arc says was not stored. Arc 2027.01+ marks such refusals with
// X-Arc-Ingest-Fenced or X-Arc-Ingest-Refused (internal/api/routing.go); older
// releases mark nothing, so there every server-side failure is reported, not
// resent (at-most-once). Header handling follows arc-client-python
// (arc_client/http/base.py ingest_refusal).

const RESENDABLE_NETWORK = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH']);

function header(headers, name) {
  if (!headers) return undefined;
  const v = headers[name.toLowerCase()];
  return Array.isArray(v) ? v[0] : v;
}

function retryAfterMs(headers) {
  const v = header(headers, 'retry-after');
  if (v === undefined) return undefined;
  const s = Number(v);
  return Number.isFinite(s) && s >= 0 ? s * 1000 : undefined;
}

function bodyError(body) {
  if (!body || body.length === 0) return '';
  try {
    const parsed = JSON.parse(body.toString('utf8'));
    if (parsed && typeof parsed.error === 'string') return parsed.error;
  } catch (_) {
    // not JSON
  }
  return body.toString('utf8').slice(0, 300);
}

// result: {status, headers, body} or {error} (a transport error carrying
// .bodySent).
// Returns {action, reason, retryAfterMs?, dropped?}
//   action: 'ok' | 'resend' | 'fail' | 'split' | 'auth'
function classify(result) {
  if (result.error) {
    const err = result.error;
    const code = err.code || '';
    if (!err.bodySent && (RESENDABLE_NETWORK.has(code) || err.tlsHandshake)) {
      return { action: 'resend', reason: `${code || err.message} (nothing sent)` };
    }
    return { action: 'fail', reason: `${code || err.message}; Arc may have stored the batch, not resent` };
  }
  const { status, headers, body } = result;
  if (status === 204 || status === 200) return { action: 'ok' };
  const text = bodyError(body);
  if (status === 503) {
    const refused = header(headers, 'x-arc-ingest-refused');
    if (refused !== undefined) {
      const reason = String(refused).trim().toLowerCase() || 'other';
      if (reason !== 'wal_not_durable') {
        return { action: 'resend', reason: `refused: ${reason}`, retryAfterMs: retryAfterMs(headers) };
      }
      return { action: 'fail', reason: 'refused: wal_not_durable; earlier rows may be stored, not resent' };
    }
    if (header(headers, 'x-arc-ingest-fenced') !== undefined) {
      return { action: 'resend', reason: 'refused: fenced', retryAfterMs: retryAfterMs(headers) };
    }
    return { action: 'fail', reason: `503 ${text || 'service unavailable'}; Arc may have stored the batch, not resent` };
  }
  if (status === 413) return { action: 'split', reason: '413 payload too large' };
  if (status === 401) return { action: 'auth', reason: `401 ${text || 'unauthorized'}` };
  if (status === 400) {
    const m = /^\s*partial write:\s*(\d+)\s+points? dropped/i.exec(text);
    if (m) return { action: 'fail', reason: text, dropped: Number(m[1]), partial: true };
  }
  return { action: 'fail', reason: `${status} ${text}`.trim() };
}

module.exports = { classify, retryAfterMs };
