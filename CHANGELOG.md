# Changelog

Notable changes to NymBroker are documented here. This changelog follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.9.2] - 2026-10-08

### Added

- Agent skills for building applications on NymBroker (`skills/`): setup, messages, consumers, routing, producer/worker, input transformers, scheduled actions, testing, observability, dead letters and troubleshooting. Download `nymbroker-skills.zip` from the GitHub release.
- Documentation: installing from NuGet, the samples and the benchmark.

### Changed

- The README's links are absolute, so they work on the nuget.org package page.

## [0.9.1] - 2026-10-07

### Changed

- `NymBroker` no longer depends on Cronos; cron expressions are parsed by a built-in, Cronos-compatible implementation.

