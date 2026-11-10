'use strict';

// Buffer and batch engine behind arc-write. Independent of Node-RED so it can
// be tested with a fake transport.
//
// Rows are buffered per (database, measurement). A key is flushed when it holds
// batchSize rows, on every flushInterval tick, on flush() and on close(). Each
// key has at most one request in flight (keeps a measurement's rows in order);
// maxInFlight caps requests across keys.
//
// Every message is tracked by a Handle. It completes (done()) only when Arc has
// answered 204 for every one of its rows, and fails (done(err)) exactly once as
// soon as any of its rows fails, is dropped, or is still buffered at close.

const { buildColumns, estimateBytes } = require('./rows');
const { encodeColumns } = require('./msgpack');
const { classify } = require('./retry');

class Handle {
  constructor(onSettle) {
    this.onSettle = onSettle;
    this.remaining = 0;
    this.settled = false;
  }

  ack(n) {
    this.remaining -= n;
    if (this.remaining <= 0 && !this.settled) {
      this.settled = true;
      this.onSettle(null);
    }
  }

  fail(err) {
    if (this.settled) return;
    this.settled = true;
    this.onSettle(err);
  }
}

class WriteError extends Error {
  constructor(message, info) {
    super(message);
    this.arc = info;
  }
}

class Writer {
  constructor(opts) {
    this.post = opts.post; // async (db, body: Buffer) -> {status, headers, body} | throws transport error
    this.authProbe = opts.authProbe; // async () -> boolean
    this.batchSize = opts.batchSize || 5000;
    this.flushIntervalMs = opts.flushIntervalMs || 1000;
    this.maxRows = opts.maxBufferRows || 200000;
    this.maxBytes = opts.maxBufferBytes || 64 * 1024 * 1024;
    this.maxPending = opts.maxPendingMessages || 100000;
    this.maxInFlight = opts.maxInFlight || 4;
    this.overflow = opts.overflow || 'drop-oldest';
    this.authProbeMs = opts.authProbeMs || 60000;
    this.maxBackoffMs = opts.maxBackoffMs || 30000;
    this.compress = opts.compress || ((b) => b);
    this.onStatus = opts.onStatus || (() => {});
    this.onBatch = opts.onBatch || (() => {});
    this.now = opts.now || Date.now;

    this.keys = new Map();
    this.rows = 0;
    this.bytes = 0;
    this.pending = 0;
    this.inFlight = 0;
    this.seq = 0;
    this.paused = false;
    this.closing = false;
    this.lastProblem = null;
    this.waiters = [];
    this.timer = setInterval(() => this.tick(), this.flushIntervalMs);
    if (this.timer.unref) this.timer.unref();
    this.retryTimer = null;
    this.probeTimer = null;
  }

  // Track a message. onSettle(err|null) is called exactly once.
  handle(onSettle) {
    this.pending++;
    return new Handle((err) => {
      this.pending--;
      onSettle(err);
    });
  }

  // groups: [{db, measurement, rows: [typedRow]}] for one message.
  enqueue(groups, handle) {
    let n = 0;
    let bytes = 0;
    const entries = [];
    for (const g of groups) {
      for (const row of g.rows) {
        const b = estimateBytes(row);
        n++;
        bytes += b;
        entries.push({ g, row, bytes: b });
      }
    }
    handle.remaining = n;
    if (n === 0) {
      handle.fail(new WriteError('message has no rows', { reason: 'empty' }));
      return;
    }
    if (n > this.maxRows || bytes > this.maxBytes) {
      handle.fail(new WriteError(`message has ${n} rows / ${bytes} bytes, more than the buffer can hold`, { reason: 'too large' }));
      return;
    }
    if (!this.makeRoom(n, bytes)) {
      handle.fail(new WriteError('buffer full; message rejected', { reason: 'buffer full', overflow: 'reject-new' }));
      return;
    }
    for (const e of entries) {
      const key = this.key(e.g.db, e.g.measurement);
      key.rows.push({ row: e.row, handle, bytes: e.bytes, seq: this.seq++ });
      this.rows++;
      this.bytes += e.bytes;
      if (key.rows.length >= this.batchSize) key.due = true;
    }
    this.pump();
    this.status();
  }

  makeRoom(n, bytes) {
    const over = () => this.rows + n > this.maxRows || this.bytes + bytes > this.maxBytes || this.pending > this.maxPending;
    if (!over()) return true;
    if (this.overflow === 'reject-new') return false;
    while (over()) {
      let oldestKey = null;
      for (const key of this.keys.values()) {
        if (key.rows.length && (!oldestKey || key.rows[0].seq < oldestKey.rows[0].seq)) oldestKey = key;
      }
      if (!oldestKey) return !over();
      const e = oldestKey.rows.shift();
      this.rows--;
      this.bytes -= e.bytes;
      e.handle.fail(new WriteError('buffer full; oldest rows dropped', {
        reason: 'buffer full', overflow: 'drop-oldest', database: oldestKey.db, measurement: oldestKey.measurement,
      }));
      this.lastProblem = 'buffer full, dropping oldest';
    }
    return true;
  }

  key(db, measurement) {
    const k = db + '\0' + measurement;
    let key = this.keys.get(k);
    if (!key) {
      key = { db, measurement, rows: [], inFlight: false, retryAt: 0, backoffMs: 1000, takeLimit: this.batchSize, due: false };
      this.keys.set(k, key);
    }
    return key;
  }

  tick() {
    for (const key of this.keys.values()) {
      if (key.rows.length) key.due = true;
      else if (!key.inFlight) this.keys.delete(key.db + '\0' + key.measurement);
    }
    this.pump();
    this.status();
  }

  flush() {
    for (const key of this.keys.values()) if (key.rows.length) key.due = true;
    this.pump();
  }

  pump() {
    if (this.paused) return;
    const now = this.now();
    let nextRetry = Infinity;
    for (const key of this.keys.values()) {
      if (this.inFlight >= this.maxInFlight) break;
      if (key.inFlight || key.rows.length === 0 || !(key.due || this.closing)) continue;
      if (key.retryAt > now) {
        nextRetry = Math.min(nextRetry, key.retryAt);
        continue;
      }
      const take = Math.min(key.rows.length, key.takeLimit);
      const entries = key.rows.splice(0, take);
      for (const e of entries) {
        this.rows--;
        this.bytes -= e.bytes;
      }
      // A tick flushes what was there; what remains waits for the next tick
      // unless it is already a full batch.
      key.due = key.rows.length >= this.batchSize;
      this.send(key, entries);
    }
    if (nextRetry !== Infinity) this.scheduleRetry(nextRetry - now);
    this.notifyWaiters();
  }

  scheduleRetry(ms) {
    if (this.retryTimer) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.pump();
    }, Math.max(ms, 10));
    if (this.retryTimer.unref) this.retryTimer.unref();
  }

  putBack(key, entries) {
    key.rows.unshift(...entries);
    for (const e of entries) {
      this.rows++;
      this.bytes += e.bytes;
    }
    key.due = true;
  }

  async send(key, entries) {
    key.inFlight = true;
    this.inFlight++;
    const started = this.now();
    let result;
    try {
      const body = this.compress(encodeColumns(key.measurement, buildColumns(entries.map((e) => e.row))));
      try {
        result = await this.post(key.db, body);
      } catch (error) {
        result = { error };
      }
    } catch (encodeErr) {
      result = { status: 0, encodeError: encodeErr };
    }
    key.inFlight = false;
    this.inFlight--;
    const durationMs = this.now() - started;
    const verdict = result.encodeError
      ? { action: 'fail', reason: 'encoding failed: ' + result.encodeError.message }
      : classify(result);
    const info = {
      database: key.db,
      measurement: key.measurement,
      rows: entries.length,
      status: result.status || (result.error && result.error.code) || null,
      reason: verdict.reason,
      durationMs,
    };
    switch (verdict.action) {
      case 'ok':
        key.backoffMs = 1000;
        key.takeLimit = this.batchSize;
        this.lastProblem = null;
        for (const e of entries) e.handle.ack(1);
        break;
      case 'resend': {
        this.putBack(key, entries);
        const wait = verdict.retryAfterMs !== undefined ? verdict.retryAfterMs : key.backoffMs;
        key.retryAt = this.now() + wait;
        key.backoffMs = Math.min(key.backoffMs * 2, this.maxBackoffMs);
        this.lastProblem = `retrying (${verdict.reason})`;
        break;
      }
      case 'split':
        if (entries.length > 1) {
          this.putBack(key, entries);
          key.takeLimit = Math.max(1, Math.floor(entries.length / 2));
          break;
        }
        this.failEntries(entries, verdict.reason, info);
        break;
      case 'auth':
        this.putBack(key, entries);
        this.pauseForAuth(verdict.reason);
        break;
      default:
        this.failEntries(entries, verdict.reason, Object.assign({ dropped: verdict.dropped, partial: !!verdict.partial }, info));
        this.lastProblem = verdict.reason;
        break;
    }
    this.onBatch(Object.assign({ action: verdict.action }, info));
    this.pump();
    this.status();
  }

  failEntries(entries, reason, info) {
    const err = new WriteError(`Arc write failed: ${reason}`, info);
    for (const e of entries) e.handle.fail(err);
  }

  pauseForAuth(reason) {
    if (this.paused) return;
    this.paused = true;
    this.lastProblem = `auth: ${reason}`;
    const probe = async () => {
      this.probeTimer = null;
      let ok = false;
      try {
        ok = this.authProbe ? await this.authProbe() : false;
      } catch (_) {
        ok = false;
      }
      if (ok) {
        this.paused = false;
        this.lastProblem = null;
        this.pump();
        this.status();
      } else if (!this.closing) {
        this.probeTimer = setTimeout(probe, this.authProbeMs);
        if (this.probeTimer.unref) this.probeTimer.unref();
      }
    };
    this.probeTimer = setTimeout(probe, this.authProbeMs);
    if (this.probeTimer.unref) this.probeTimer.unref();
    this.notifyWaiters();
  }

  status() {
    let state = 'ok';
    if (this.paused) state = 'auth';
    else if (this.lastProblem && this.lastProblem.startsWith('retrying')) state = 'retrying';
    else if (this.lastProblem) state = 'error';
    this.onStatus({ state, buffered: this.rows, inFlight: this.inFlight, pending: this.pending, text: this.lastProblem });
  }

  idle() {
    return this.rows === 0 && this.inFlight === 0;
  }

  // Nothing more will happen before deadline without outside help.
  stalled(deadline) {
    if (this.inFlight > 0) return false;
    if (this.paused) return true;
    for (const key of this.keys.values()) {
      if (key.rows.length && key.retryAt <= deadline) return false;
    }
    return true;
  }

  notifyWaiters() {
    const w = this.waiters;
    this.waiters = [];
    for (const fn of w) fn();
  }

  waitChange(ms) {
    return new Promise((resolve) => {
      const t = setTimeout(resolve, ms);
      this.waiters.push(() => {
        clearTimeout(t);
        resolve();
      });
    });
  }

  // Flush everything, waiting at most timeoutMs. Rows still buffered at the
  // deadline fail their messages.
  async close(timeoutMs) {
    this.closing = true;
    clearInterval(this.timer);
    const deadline = this.now() + timeoutMs;
    this.flush();
    while (!this.idle() && this.now() < deadline && !this.stalled(deadline)) {
      this.pump();
      await this.waitChange(Math.min(100, Math.max(1, deadline - this.now())));
    }
    if (this.retryTimer) clearTimeout(this.retryTimer);
    if (this.probeTimer) clearTimeout(this.probeTimer);
    const left = [];
    for (const key of this.keys.values()) {
      for (const e of key.rows) left.push(e);
      key.rows = [];
    }
    this.rows = 0;
    this.bytes = 0;
    if (left.length) {
      const err = new WriteError(`node closed with ${left.length} rows still buffered`, { reason: 'closed', rows: left.length });
      for (const e of left) e.handle.fail(err);
    }
    return { unsent: left.length, inFlight: this.inFlight };
  }
}

module.exports = { Writer, WriteError };
