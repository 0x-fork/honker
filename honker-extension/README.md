# honker-extension

SQLite loadable extension for [Honker](https://honker.dev). Adds every `honker_*` SQL scalar function (queues, streams, scheduler, pub/sub, rate limits, locks, results) to any SQLite 3.9+ client.

## Install

From crates.io (builds `libhonker_ext.dylib` / `.so` for your platform):

```bash
cargo install honker-extension
# or build from source:
cargo build --release -p honker-extension
# → target/release/libhonker_ext.{dylib,so}
```

Prebuilt binaries per platform are available at [GitHub releases](https://github.com/russellromney/honker/releases/latest).

## Use

```sql
.load ./libhonker_ext
SELECT honker_bootstrap();

-- Queues
SELECT honker_enqueue('emails', '{"to":"alice"}', NULL, NULL, 0, 3, NULL);
SELECT honker_claim_batch('emails', 'worker-1', 32, 300);
SELECT honker_ack_batch('[1,2,3]', 'worker-1');

-- Streams (durable pub/sub)
SELECT honker_stream_publish('orders', 'k', '{"id":42}');
SELECT honker_stream_read_since('orders', 0, 1000);

-- pg_notify-style pub/sub
SELECT notify('orders', '{"id":42}');
```

Full SQL reference: [honker.dev/reference/extension](https://honker.dev/reference/extension/).

## License

Apache-2.0.

### SQL call context

Call `honker_claim_batch`, `honker_fail`, `honker_sweep_expired`, and
`honker_retry` through a **separate SELECT**, after finishing any write cursors
on that connection. Do not put them inside a trigger, an INSERT/UPDATE/DELETE,
or a RETURNING expression. An unfinished `INSERT ... RETURNING` cursor also
blocks these calls, even when the call itself is a separate SELECT.

These operations use savepoints to preserve jobs if part of the operation fails.
SQLite cannot open a savepoint while a write statement is active. Increasing
`busy_timeout` does not resolve this condition: consume or close the write cursor.
An explicit application transaction is supported and keeps all work atomic:

```sql
BEGIN;
UPDATE app_orders SET status = 'failed' WHERE id = 42;
-- If using RETURNING above, finish its cursor before the next statement.
SELECT honker_fail(7, 'worker-1', 'delivery rejected');
COMMIT;
```

The application must roll back the transaction on an error. This restriction
also applies to direct Rust calls while their connection has an active write
statement. It is a compatibility change from versions before savepoint-protected
job transitions; keep the operations separate instead of embedding them in DML.
