# Security Policy

## Supported versions

Cygnus is pre-release. Until `1.0`, only the latest commit on `main` receives fixes.

## Reporting a vulnerability

Please report privately via [GitHub Security Advisories][advisories], or by email to
ebnsina.me@gmail.com. Do not open a public issue.

[advisories]: https://github.com/ebnsina/cygnus/security/advisories/new

Please include a description of the issue, the steps to reproduce it, the affected version
or commit, and the impact as you see it.

You can expect an acknowledgement within 72 hours and an assessment within seven days.
Fixes are released as soon as they are ready; credit is given unless you prefer otherwise.

## Scope notes

Cygnus executes job handlers that you write, against arguments stored in your database. Two
consequences are worth stating plainly:

- **Job arguments are untrusted input** if any part of your system lets external users
  influence them. Validate them in your handler exactly as you would an HTTP request body.
- **The web UI exposes job arguments and error messages**, which frequently contain
  sensitive data. It ships with no authentication by design, so that you can supply your
  own. Never mount it on a public route without an auth middleware in front of it.
