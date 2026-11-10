# @basekick-labs/node-red-arc

Node-RED nodes for [Arc](https://github.com/Basekick-Labs/arc). Write rows from sensors, PLCs, MQTT or anything else in a flow to Arc as MessagePack, with batching, buffering and safe retries, and read them back with SQL.

```
npm install @basekick-labs/node-red-arc
```

Requires Node-RED 4.0 or later. No runtime dependencies.

## Nodes

| Node | What it does |
|---|---|
| **arc-server** | Connection: URL, token, default database, optional TLS. Shared by the other nodes. |
| **arc write** | Buffers rows per measurement and sends them to `/api/v1/write/msgpack` in batches. |
| **arc query** | Runs SQL and returns the rows. |

## Writing

Every value is a column — there are no tags or fields.

```js
msg.measurement = "boiler";
msg.payload = { sensor: "a1", line: 3, temperature: 21.5, ok: true };
```

`msg.payload` can also be an array of rows, `{rows: [...]}`, `{columns: {name: [...]}}` or a single number (column `value`). Rows take their time from a `time` key or `msg.time`: a Date, an ISO 8601 string (UTC when it has no offset), a number in the node's time unit, or a BigInt / digit string for exact nanoseconds. Otherwise the time they arrived. Times more than 50 years from now are rejected as a likely unit mistake.

**A message is finished only when Arc has stored it.** The node calls `done()` after Arc answers 204 for every row of the message, so a Complete node fires when the data is in Arc, typically one flush interval later. A message that could not be written goes to Catch nodes with its payload untouched and `msg.arc` added, saying why.

### Types

- Every number is written as a float (`DOUBLE`), even `21`, so a reading that is sometimes whole and sometimes not stays one column. Arc fixes a column's type from the first value it sees in a batch and would otherwise truncate later decimals.
- List integer columns in *Integer columns*, or send a `BigInt`, to get `BIGINT`. A fractional value for an integer column rejects the message rather than being truncated.
- Strings and booleans as they are; `null`, `undefined`, `NaN` and `Infinity` are an empty cell.
- The first value of a column fixes its type for the node. A later value of another type (say `"N/A"` in a numeric column) leaves that cell empty and keeps the rest of the row, or rejects the message if you choose so; the value is listed in `msg.arc.rejectedCells`.

### Names

Database and measurement names start with a letter and contain letters, digits, `_` and `-`. Column names must not start with `_`, and only the time column may be called `time` (in any case). Arc treats names that differ only in A–Z case as one column, so the first spelling the node sees for a measurement is kept and a message using another spelling is rejected. Invalid messages fail immediately and are never sent, so they cannot block other rows. *Replace invalid characters* turns `plant/line 3` into `plant_line_3`.

### Failures and retries

The node resends a batch only when Arc says nothing was stored:

| Arc answer | What the node does |
|---|---|
| 204 | done |
| 503 with `X-Arc-Ingest-Fenced` / `X-Arc-Ingest-Refused` (Arc 27.01+) | waits `Retry-After` and resends |
| connection refused, DNS failure | backs off (1 s to 30 s) and resends |
| connection reset or timeout before the request body was sent | resends |
| 401 | pauses the node and checks the token every 60 s |
| 413 | splits the batch and resends the halves |
| 400 `partial write` (Arc 27.01+) | reports the batch; most of its rows *were* stored, `msg.arc.partial` is true and `msg.arc.dropped` says how many were not |
| anything else (400, 403, 500, 502, 504, a reset or timeout after sending, any 503 from Arc before 27.01) | reports the batch to Catch nodes, never resends |

A batch is never sent twice, so rows are never duplicated. On Arc releases before 27.01 a server-side failure cannot be told apart from "stored", so it is reported instead of retried.

### Buffer

Rows wait in memory, per database and measurement, until *Batch size* rows are buffered or *Flush every* has passed. When the buffer is full the oldest message is dropped whole and fails (or new messages are rejected). Size it as rows per second × the longest Arc outage you want to ride out. When a flow is stopped or redeployed, buffered rows are flushed within the node close timeout; whatever is left fails its messages.

## Querying

```sql
SELECT avg(temperature) FROM boiler WHERE sensor = {{sensor}} AND time > now() - INTERVAL '1 hour'
```

`{{name}}` takes `msg.params.name` and quotes it as an SQL literal (`{{#name}}` for an identifier). Arc has no query parameters, so do not build SQL by concatenating message values; `msg.sql` is off unless you enable it. Output modes: an array of row objects, one message per row, chunks of N rows, or `{columns, data}`. The `time` column is an ISO string with microseconds.

## Development

```
npm test                                  # unit and node tests
ARC_MAIN_BIN=/path/to/arc npm run e2e     # Node-RED 4 and 5 in Docker against Arc 26.09.3 (+ a native Arc build)
```

## License

MIT
