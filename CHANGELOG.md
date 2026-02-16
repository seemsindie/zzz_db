# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-02-16

### Added
- SQLite and PostgreSQL backends with compile-time dialect selection
- Compile-time schema definitions with automatic DDL generation
- Type-safe repository pattern (all, one, get, insert, update, delete, count, deleteAll, updateWhere)
- Composable query builder with where, joins, groups, aggregates, ordering, limit/offset
- Connection pooling with configurable size and timeouts
- Transaction support with nested savepoints and isolation levels
- Versioned migration system with up/down functions and DDL helpers
- Changeset validation and change tracking
- Testing utilities (TestSandbox, Factory, seed)
- Dialect-aware SQL generation (? for SQLite, $N for PostgreSQL)
- GitHub Actions CI with PostgreSQL service container
