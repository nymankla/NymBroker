# Versioning and API compatibility

## Versioning

All NymBroker NuGet packages use the single `<Version>` in `Directory.Build.props`. Keep the core, endpoint, and idempotency packages in lockstep; `scripts/pack.ps1` packs every library with that version. The project is in the `0.x` pre-1.0 channel. Use `0.minor.patch` versions: patches are backward-compatible fixes, while a minor version may contain an intentional breaking change before 1.0. Once 1.0 is declared, follow Semantic Versioning for stable APIs.

Tag each release as `v<version>` (for example, `v0.6.0`). CI runs on `v*` tags and fails if the tag does not exactly match `Directory.Build.props`. Update `CHANGELOG.md` and the shared version before creating the tag; do not move or reuse a release tag. Package publishing remains a deliberate release step after the tagged build and package verification.

## Public API guardrails

Every packable NymBroker library tracks its public surface in `PublicAPI.Shipped.txt` and `PublicAPI.Unshipped.txt`. CI treats the API baseline and nullability diagnostics as errors, so adding or removing a public API requires an explicit baseline update. `EnablePackageValidation` also runs the .NET SDK package validation checks during packing.

For a new API, keep its declaration in `PublicAPI.Unshipped.txt` while it is under development. When preparing a release, review that file and move accepted declarations into `PublicAPI.Shipped.txt`. For an approved removal, move the shipped declaration to the unshipped file with the analyzer's `*REMOVED*` prefix. Do not delete shipped declarations to silence a diagnostic: removal is a breaking change and must follow the policy below.

## Breaking changes and deprecation

Prefer additive changes. In the 0.x channel, a breaking change must be intentional, documented in the changelog, and released in a minor version; patches must not remove or alter existing public contracts. After 1.0, breaking changes require a major version.

When feasible, mark an API for removal with `[Obsolete]` first, explain the replacement in its message and documentation, and retain it through at least one subsequent minor release before removal. A security or correctness issue may require an exception, which must be called out in the changelog. Keep all packages at the same version when making a release.
