---
name: make-release
description: Release checklist for NymBroker — version, changelog, PublicAPI files, tag, CI publish. Use only when the user asks for a release and has chosen the version level.
---

# Release checklist

Only when the user asks for a release and has chosen the level:

1. `<Version>` in `Directory.Build.props` (edit the line directly; `pack.ps1 -Version` also works but drops blank lines — check the diff).
2. `CHANGELOG.md`: rename `[Unreleased]` content to `## [x.y.z] - YYYY-MM-DD`, keep an empty `[Unreleased]` above it.
3. Every `PublicAPI.Unshipped.txt`: move added lines to `PublicAPI.Shipped.txt`; for each `*REMOVED*<line>`, delete `<line>` from `Shipped`; leave `Unshipped` as `#nullable enable`. Build must stay at 0 warnings.
4. Commit "Release x.y.z", `git tag -a vx.y.z -m "NymBroker x.y.z"`, push `master`, then push the tag.
5. Watch the CI run (`gh run list --branch vx.y.z`, `gh run watch`): build, publish to nuget.org (trusted publishing), GitHub release with `nymbroker-skills.zip`. nuget.org lists the version a few minutes later.
