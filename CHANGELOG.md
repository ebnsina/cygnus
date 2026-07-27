# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x`, the public API may change in any release. Breaking changes are
called out explicitly under a **Changed** heading and will always appear in this file
before they appear in a tag.

## [Unreleased]

### Added

- Initial repository scaffolding: Go module, MIT license, contributor documentation.
- PostgreSQL development environment via Docker Compose, and a Makefile covering build,
  test, migrate, and benchmark workflows.
- Feasibility-spike schema: the `cygnus_job` table plus the partial indexes backing the
  fetch, scheduler, and rescuer paths.
- Spike queue operations: batched `COPY` insert, single-round-trip `SKIP LOCKED` fetch
  with leasing, and completion guarded on the job still being in `running`.

[Unreleased]: https://github.com/ebnsina/cygnus/commits/main
