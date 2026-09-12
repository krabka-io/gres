<p align="center">
  <a href="https://github.com/krabka-io/gres/actions/workflows/ci.yml"><img src="https://github.com/krabka-io/gres/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/krabka-io/gres"><img src="https://codecov.io/gh/krabka-io/gres/graph/badge.svg" alt="codecov"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="Apache-2.0"></a>
</p>

# Gres

Gres is a pure-Rust, PostgreSQL-compatible SQL engine. It provides a PostgreSQL
wire server, parser, type system, MVCC storage, system catalog, SQL executor,
and a differential conformance harness against PostgreSQL 18.

Gres is beta, pre-1.0 software. It is suitable for development, compatibility
testing, and non-critical workloads while SQL breadth and operational hardening
continue. It does not promise storage compatibility between releases yet.

## Architecture

| Area | Crates |
| --- | --- |
| Server | [`gres`](crates/gres) |
| Wire protocol | [`pgwire`](crates/pgwire) |
| Parser and values | [`pgparser`](crates/pgparser), [`pgtypes`](crates/pgtypes) |
| Execution | [`pgexec`](crates/pgexec), [`pgcatalog`](crates/pgcatalog) |
| Storage and MVCC | [`pgkv`](crates/pgkv), [`pgmvcc`](crates/pgmvcc) |
| Distributed runtime | [`gres-substrate`](crates/gres-substrate), [`gres-ranges`](crates/gres-ranges), [`gres-control`](crates/gres-control) |
| PostgreSQL differential tests | [`gres-conformance`](crates/gres-conformance) |

The local server runs either in memory or on a durable local data directory.
The optional substrate mode stores tenant WAL in an external replicated log and
rebuilds a disposable local read model during recovery.

Rust package names and environment variables retain their existing
`crabka-` and `CRABKA_` prefixes for compatibility. In prose, the parent project
is named krabka.

## Quick start

The pinned Rust toolchain is declared in
[`rust-toolchain.toml`](rust-toolchain.toml).

```bash
git clone https://github.com/krabka-io/gres.git
cd gres
cargo build --locked -p crabka-gres
cargo run --locked -p crabka-gres -- --listen 127.0.0.1:5433 --auth trust
```

Connect with any PostgreSQL client:

```bash
psql -h 127.0.0.1 -p 5433 -U postgres
```

`--auth trust` is for local development only. To test password authentication:

```bash
cargo run --locked -p crabka-gres -- \
  --listen 127.0.0.1:5433 \
  --auth scram \
  --user-cred app=change-me
```

Persist the local database across restarts with `--data-dir`:

```bash
cargo run --locked -p crabka-gres -- \
  --listen 127.0.0.1:5433 \
  --auth trust \
  --data-dir target/gres-data
```

An in-process substrate is available for development without an external log:

```bash
cargo run --locked -p crabka-gres -- \
  --listen 127.0.0.1:5433 \
  --substrate-bootstrap memory:// \
  --tenant demo \
  --auth trust \
  --cache-dir target/gres-cache
```

Run `cargo run -p crabka-gres -- --help` for TLS, SCRAM, checkpoint,
multi-range, and runtime-limit options.

## Compatibility

Gres targets PostgreSQL behavior at the SQL, catalog, error, transaction, and
wire-protocol layers. PostgreSQL page files, physical WAL, extensions written in
C, and physical replication SQL are not compatibility goals.

The detailed feature inventory and known divergences are maintained in the
[`PostgreSQL compatibility matrix`](docs/PG_COMPAT_MATRIX.md). The conformance
harness compares results and SQLSTATEs with PostgreSQL 18 and ratchets committed
baselines so compatibility cannot silently regress.

Run the pinned upstream `pg_regress` suite with:

```bash
./scripts/gres-pg-regress.sh self-check both
./scripts/gres-pg-regress.sh gres serial
./scripts/gres-pg-regress.sh gres parallel
```

See the [`gres-conformance` guide](crates/gres-conformance/README.md) for build
prerequisites, artifacts, baseline updates, and diagnostic corpus runs.

## Development

Bazel is the CI build and test path for the Gres core. Cargo manifests and
`Cargo.lock` remain the dependency source of truth, and `MODULE.bazel.lock`
pins the generated Bazel graph.

Run the same core checks used by CI:

```bash
cargo +nightly-2026-08-14 fmt --all -- --check
cargo clippy \
  -p crabka-units -p crabka-trace-context -p crabka-pgtypes \
  -p crabka-pgparser -p crabka-pgwire -p crabka-pgkv \
  -p crabka-pgmvcc -p crabka-pgcatalog -p crabka-pgexec \
  -p crabka-gres-conformance --all-targets

bazel test \
  //crates/units/... //crates/trace-context/... //crates/pgtypes/... \
  //crates/pgparser/... //crates/pgwire/... //crates/pgkv/... \
  //crates/pgmvcc/... //crates/pgcatalog/... //crates/pgexec/... \
  //crates/gres-conformance/...

cargo nextest run -p crabka-pgexec --test telemetry --test telemetry_exec
```

The telemetry suites use Nextest because each test needs its own process-global
tracing subscriber. All ordinary unit, documentation, and integration tests run
through Bazel.

## Mutation testing

The weekly mutation workflow covers the parser, wire layer, storage, catalog,
and executor. Generated mutation targets include each crate's ordinary
integration tests in addition to its unit-test binary.

Run one shard locally with:

```bash
tools/check-mutants.sh //crates/pgtypes:pgtypes_mutants 0 16
```

Inspect the generated target when changing test coverage:

```bash
bazel query //crates/pgtypes:pgtypes_mutants --output=build
```

## Contributing

Open an issue before a large design or compatibility change. Keep behavioral
changes paired with focused tests. Corpus growth and conformance-baseline changes
must land together with the parity evidence that explains the new floor.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for repository conventions.

## Security

Do not use trust authentication outside local development. Production-facing
deployments should configure TLS and SCRAM, and multi-range RPC endpoints require
mTLS plus an explicit principal allowlist.

Report vulnerabilities through GitHub private vulnerability reporting rather
than a public issue.

## License

Gres is licensed under the Apache License, Version 2.0. See
[`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
