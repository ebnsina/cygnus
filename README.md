# Cygnus

Transaction-safe background jobs for Go, backed by PostgreSQL.

> **Status: pre-alpha.** Nothing here is stable and there is no release yet. The public
> API does not exist. The current work is a feasibility spike validating the storage
> engine's performance characteristics. Do not use this in production.

## Why

Enqueue a job inside the same transaction as your business data, and the job becomes
visible only if that transaction commits:

```go
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

user, err := createUser(ctx, tx, input)
if err != nil {
    return err
}

// No dual-write. No lost jobs. No jobs referencing rows that were rolled back.
if _, err := client.InsertTx(ctx, tx, SendWelcomeArgs{UserID: user.ID}, nil); err != nil {
    return err
}

return tx.Commit(ctx)
```

Planned properties:

- **Driver-agnostic** — works with `pgx/v5` *and* `database/sql`, so sqlx, GORM, and bun
  users are first-class rather than second-class.
- **Type-safe** — job arguments arrive at your handler already typed, via generics.
- **Dependency-light** — the core module pulls in no third-party packages.
- **MIT, in full** — workflows, concurrency limits, rate limiting, and multi-tenancy are
  part of the library, not a paid tier.

## What Cygnus is not

Cygnus is a **job queue with a dependency DAG**, not a durable execution engine. There is
no event-history replay, so your handlers are ordinary Go code with no determinism
constraints — but there is also no dynamic runtime branching, no signals, and no workflow
versioning. If you need those, use [Temporal](https://temporal.io); it is a different
category of system and it is very good at what it does.

## Requirements

- Go 1.26+
- PostgreSQL 14+

## Development

```sh
make db-up        # start PostgreSQL in Docker
make migrate      # apply the schema
make test         # run tests
make bench        # run the throughput benchmark
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
