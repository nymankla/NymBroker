---
name: code-health
description: Measure NymBroker code health — method length, cyclomatic and cognitive complexity, nesting, parameter counts, type/file size, cross-file duplication — and report ranked refactoring opportunities. Use when the user asks about code health, code quality, complexity, function size, hotspots, technical debt or refactor candidates. Read-only: it changes no code unless the user then asks for a refactor.
---

# Code health

Measure, read the hotspots, recommend. Do **not** change code in this skill; refactors are a follow-up the user picks.

## 1. Measure

The analyzer is a .NET file-based app (Roslyn). Copy both scripts to the scratchpad (or `$env:TEMP`) before running, so the repo's `Directory.Build.props` (analyzers, warnings as errors) does not apply to it:

```bash
D=<scratchpad>/code-health; mkdir -p $D
cp .claude/skills/code-health/scripts/* $D/
cd $D && dotnet run health.cs -- E:/utv/NymBroker health.json   # prints files=, methods=, types=, dupGroups=
node $D/summarize.js $D/health.json
```

What it measures (per method, accessor of 5+ lines, and local function; `bin/`, `obj/`, `.claude/` skipped):

| Field | Meaning |
|---|---|
| `lines` | declaration span incl. signature |
| `cc` | cyclomatic: 1 + if/loops/case/arm/catch/`?:`/`&&`/`\|\|`/`??`/`?.` |
| `cognitive` | Sonar-style approximation: branches +1 plus nesting level, `else` +1, each run of `&&`/`\|\|` +1 |
| `nesting` | deepest control-flow nesting |
| `parameters`, `awaits` | counts |

Types: lines, members, fields, largest constructor. Duplication: 10 normalized code lines (no braces, `using`, comments) hashed across production files; tests and samples are excluded from that scan.

## 2. Judge against these thresholds

Report production code (`NymBroker.Core`, endpoints, stores, `shared/`) separately from `NymBroker.Tests` and `samples/`; only production drives recommendations.

| Metric | Watch | Act |
|---|---|---|
| Method lines | > 50 | > 100 |
| Cyclomatic | > 10 | > 20 |
| Cognitive | > 15 | > 25 |
| Nesting | ≥ 4 | |
| Parameters | ≥ 6 (DI constructors excepted) | |

Give the distribution (p50/p90/p99/max and counts over threshold) — it shows whether a problem is a few hotspots or spread out. Compare with earlier numbers if the user has them.

Baselines (production code):

| When | Methods | Max lines | Max cc | Max cognitive | cog > 15 | Duplicate groups |
|---|---|---|---|---|---|---|
| 0.22.1 (Nov 2026) | 761 | 119 | 36 | 38 (`RouteAndDispatchAsync`) | 10 | 8 |
| After the Nov 2026 refactor | 781 | 63 | 16 | 20 (`CronSchedule.FindNext`) | 4 | 5 |

## 3. Read before recommending

Open every method in the top ~6 by cognitive complexity and length; never recommend from numbers alone. For each, name a concrete, behaviour-preserving change:

- Existing `// ── Section ──` comments in `NymBrokerImpl` mark natural extraction seams.
- Repeated decision logic (e.g. the native-dead-letter / no-retry / post-to-dead-letter rule) → one helper; the rule is the table in the root `CLAUDE.md`.
- Parameter groups that travel together → a small record.
- High score from flat loops (e.g. `NymBrokerBuilder.Build`) is lower risk — say so and rank it lower.
- Duplication between endpoint or store packages → `shared/` (see `shared/README.md`); duplicated tests across stores → a contract base class like `ScheduleStoreContractTests`.

## 4. Report

1. Short verdict.
2. Production scorecard table, plus one line each for tests and samples.
3. Ranked refactoring opportunities with clickable `file:line`, metrics, and the specific change.
4. Note that refactors touching the broker pipeline, an endpoint or a store need `./scripts/test-all.ps1` (definition of done).
5. Offer, without doing it: a regression guard using the SDK's CA1502 (complexity) / CA1505 (maintainability) via `.editorconfig` + `CodeMetricsConfig.txt` with thresholds just above the current worst — a build-configuration change, so it needs the user's sign-off.
6. Scratchpad paths of `health.json` for re-runs.
