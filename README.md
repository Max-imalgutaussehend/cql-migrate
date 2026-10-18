# cql-migrate

**Schema migrations for Cassandra and ScyllaDB.** Part of the
[cqlz family](https://max-imalgutaussehend.github.io/cqlz/cql-migrate.html). Single static binary, no JVM.

```sh
cargo install cql-migrate

cql-migrate new create_users          # migrations/0001_create_users.cql
cql-migrate verify                    # replay the whole chain offline (cqlc-core): catches ALTERs of unknown tables, ...
cql-migrate up                        # apply pending migrations in order
cql-migrate status                    # applied / pending / CHANGED / FAILED
cql-migrate diff schema/ --write add_email   # generate the next migration from a desired schema
cql-migrate drift                     # does the live cluster still match the migrations?
```

## What it does

* **Ordered files** `0001_name.cql`, `0002_name.cql`, ... Each is a list of CQL statements (a `BEGIN ... APPLY BATCH` block counts as one).
* **History table** `cql_migrate.history` records version, name, checksum, state, time and duration. The keyspace and its replication
  are configurable (`history-keyspace`, `history-replication`) because production needs `NetworkTopologyStrategy`.
* **Checksums** are computed over the token stream, so comments, whitespace and keyword case do not matter (run
  [`cqlint format`](https://github.com/Max-imalgutaussehend/cqlint) freely), but any real change to an applied migration is refused.
* **Cluster-wide lock** (a lightweight transaction with a TTL) keeps two CI jobs from migrating at once; a crashed runner cannot block forever.
* **Schema agreement** is awaited after every DDL statement.
* **Honest failure handling.** CQL DDL is not transactional. If statement 3 of 5 fails, the migration is recorded as `failed` with the
  statement number and error, `up` refuses to continue, and you decide: fix the file and `resolve N --retry`, or finish by hand and
  `resolve N --applied`. Use `IF NOT EXISTS` / `IF EXISTS` so retries are safe (`cqlint check --select CQLINT010 --fix` adds them).
* **Offline chain check** (`verify`, also run by `up`): every migration is replayed with
  [`cqlc-core`](https://github.com/Max-imalgutaussehend/cqlc), so `ALTER TABLE` of a table that does not exist yet, duplicate columns or
  invalid types fail before they reach a cluster.
* **`diff`** compares the schema the migrations produce with a desired schema (for example the `schema/` directory of a
  [cqlc](https://github.com/Max-imalgutaussehend/cqlc) project) and writes the missing `CREATE TABLE` and `ALTER TABLE ... ADD`
  statements. Drops are written as comments unless `--allow-destructive`; type and primary-key changes are reported as impossible in
  place (Cassandra cannot do them). Exit code 1 when there is a difference: use it in CI to keep `schema/` and `migrations/` in sync.
* **`drift`** reads `system_schema` and reports tables and columns that exist only in the cluster or only in the migrations, and type
  differences.

## Configuration (`cql-migrate.toml`)

```toml
node = "127.0.0.1:9042"
single-node = false            # true for Docker port mappings and SSH tunnels
migrations = "migrations"
history-keyspace = "cql_migrate"
history-replication = "{'class': 'NetworkTopologyStrategy', 'dc1': 3}"
lock-ttl = 600
keyspaces = ["shop"]           # which keyspaces `drift` compares (default: those the migrations create)
```

Flags and environment: `--node` / `CQL_NODE`, `--single-node`, `--migrations`, credentials `CQL_USER` and `CQL_PASSWORD`.

## In CI

```yaml
- run: cargo install cql-migrate
- run: cql-migrate verify
- run: cql-migrate diff schema/        # fails when schema/ and migrations/ disagree
- run: cql-migrate up
  env: { CQL_NODE: cassandra.internal:9042, CQL_USER: ${{ secrets.CQL_USER }}, CQL_PASSWORD: ${{ secrets.CQL_PASSWORD }} }
```

## Tested

Unit tests for parsing, checksums, replay and diff; integration tests run the binary against ScyllaDB and Apache Cassandra 5.0 in CI
(full lifecycle, checksum protection, failure and resolve, lock, drift detection).

## Not (yet) covered

TLS settings, rollbacks (`down`; DDL cannot be rolled back reliably, write a new migration), index and UDT diffing, data migrations
with progress tracking.

Apache-2.0
