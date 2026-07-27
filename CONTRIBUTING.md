# Contributing to Cygnus

Thanks for your interest. This document covers how to get set up and what the project
expects from a change.

## Getting set up

```sh
git clone git@github.com:ebnsina/cygnus.git
cd cygnus
make db-up      # PostgreSQL 17 in Docker on port 5434
make migrate
make test
```

Tests need a running PostgreSQL. `make db-up` provides one that does not collide with a
local install on 5432. Point elsewhere with `CYGNUS_DATABASE_URL` if you prefer.

## Project layout

| Path | Purpose |
|---|---|
| `spike/` | Feasibility spike. Validates performance assumptions; not the public API. |
| `spike/schema` | Schema and a minimal migrator |
| `spike/queue` | `SKIP LOCKED` fetch loop and batch insert |
| `spike/listen` | `LISTEN`/`NOTIFY` backends for pgx, pgx/stdlib, and lib/pq |
| `spike/bench` | Throughput and latency harness |
| `cmd/spike` | CLI driving the above |

The spike is deliberately separate from the eventual public API. Code graduates out of it
once its assumptions are proven, and the package is deleted when it has served its purpose.

## Design constraints

These are not style preferences. A change that violates one will be asked to change.

1. **The core module has no third-party dependencies.** Anything needing `pgx`, `lib/pq`,
   a logger, or a metrics client belongs in a submodule. This is the project's single most
   important constraint — it is why someone can adopt Cygnus without auditing a dependency
   tree.
2. **All timestamps come from the database**, via `now()`. Never from the Go process. Client
   clocks drift, and every bug that causes is miserable to diagnose.
3. **No unbounded `DELETE` or `UPDATE`.** Maintenance work is batched with a `LIMIT` and
   loops. One unbounded statement against a large table locks up a production database.
4. **Errors are returned, never panicked.** A panic inside a job handler is recovered,
   recorded, and treated as a job failure. It must never take down the worker process.
5. **API documentation is verified against upstream sources**, not written from memory.

## Making a change

- Branch from `main`.
- Keep commits focused; one logical change each.
- Add an entry to `CHANGELOG.md` under `## [Unreleased]` for anything user-visible.
- Run `make check` (build, vet, race tests) before opening a pull request.

### Commit messages

[Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <subject>
```

Types: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`.

```
feat(listen): support LISTEN over database/sql connections
fix(queue): prevent double-fetch when a lease expires mid-transaction
```

## Testing

- Tests run under `-race` in CI. Concurrency bugs are the failure mode that matters most.
- Anything touching SQL needs a test against a real PostgreSQL. No mocks for the database.
- Performance-sensitive queries need a plan assertion, so a regression that turns an index
  scan into a sequential scan fails CI rather than being discovered in production.

## Scope

Cygnus is a job queue with a dependency DAG. It is not a durable execution engine.

The rule: **if a feature would require Cygnus to re-execute your code to reconstruct state,
it is out of scope.** That excludes event-history replay, signals and queries, dynamic
runtime branching, and workflow versioning. This boundary is what keeps the project
finishable, so it is enforced strictly and without prejudice to the idea itself.

## Reporting bugs

Include the PostgreSQL version, Go version, driver (`pgx` or `database/sql`), and a
reproduction. For a race or a stuck job, include the relevant rows from the job table with
their `state`, `attempt`, and `lease_expires_at`.
