---
name: code-health
description: Measure NymBroker code health — method length, cyclomatic and cognitive complexity, nesting, parameter counts, type/file size, cross-file duplication — and report ranked refactoring opportunities. Use when the user asks about code health, code quality, complexity, function size, hotspots, technical debt or refactor candidates, or when CodeHealthTests fails. Read-only: it changes no code unless the user then asks for a refactor.
---

# Code health

Measure, read the hotspots, recommend. Do **not** change code in this skill; refactors are a follow-up the user picks.

The metrics live in one place: `NymBroker.Tests/CodeHealth/CodeMetrics.cs` (Roslyn, syntax only). `CodeHealthTests` uses them as a gate in every `dotnet test` and in CI; this skill uses the same code for the full report. Limits, the ratchet rule and what each metric counts: [docs/architecture.md#code-health](../../../docs/architecture.md#code-health).

## 1. Measure

```bash
R=<scratchpad>/code-health.json
NYMBROKER_CODE_HEALTH_REPORT="$R" dotnet test --project NymBroker.Tests -- --filter-class "*CodeHealthTests"
node .claude/skills/code-health/scripts/summarize.js "$R"
```

(PowerShell: `$env:NYMBROKER_CODE_HEALTH_REPORT = $R` first.) The test writes the report before asserting, so it is there even when the gate fails. `NYMBROKER_CODE_HEALTH_SUMMARY=<file>` appends the Markdown summary CI puts in the job summary.

The report has every method (`lines`, `cc`, `cognitive`, `nesting`, `parameters`, `awaits`, `file:line`), type (lines, members, fields, largest constructor), file (lines, code lines) and duplicated 10-line block across production files.

## 2. Judge

Report production code (Core, endpoints, stores, `shared/`) separately from `NymBroker.Tests` and `samples/`; only production drives recommendations.

- **Gate limits** (`CodeHealthTests` constants): a method over one fails the build — name it first.
- **Watch** list for planning: cognitive > 15, cyclomatic > 10, > 50 lines, nesting ≥ 4, ≥ 6 parameters (DI constructors excepted).
- Give the distribution (p50/p90/p99/max, counts over the watch levels): it shows whether a problem is a few hotspots or spread out. Compare with the baselines below.

| When | Methods | Max lines | Max cc | Max cognitive | cog > 15 | Duplicate groups |
|---|---|---|---|---|---|---|
| 0.22.1 (Nov 2026) | 761 | 119 | 36 | 38 (`RouteAndDispatchAsync`) | 10 | 8 |
| After the Nov 2026 refactor (gate added) | 781 | 63 | 16 | 20 (`CronSchedule.FindNext`) | 4 | 5 |

## 3. Read before recommending

Open every method in the top ~6 by cognitive complexity and length; never recommend from numbers alone. For each, name a concrete, behaviour-preserving change:

- Separate stages into methods named after what they do (as the pipeline in `NymBrokerImpl.Processing.cs` now is).
- Repeated decision logic → one helper (e.g. `SettlementFor` / `SettleFailureAsync` for the settlement rule).
- Parameter groups that travel together → a small private record (e.g. `CronField`).
- High score from flat loops is lower risk — say so and rank it lower.
- Duplication between endpoint or store packages → `shared/` (see `shared/README.md`); duplicated tests across stores → a contract base class (`ScheduleStoreContractTests`, `IdempotencyStoreContractTests`).

## 4. Report

1. Short verdict, and whether `CodeHealthTests` passes.
2. Production scorecard table, plus one line each for tests and samples.
3. Ranked refactoring opportunities with clickable `file:line`, metrics, and the specific change.
4. Refactors touching the broker pipeline, an endpoint or a store need `./scripts/test-all.ps1`; hot-path changes the benchmark before and after ([docs/benchmarks.md](../../../docs/benchmarks.md)).
5. If a refactor lowered a worst value, remind that the matching limit in `CodeHealthTests` should be lowered (ratchet), and add a baseline row above.
