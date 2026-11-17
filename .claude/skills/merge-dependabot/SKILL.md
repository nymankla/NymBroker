---
name: merge-dependabot
description: Merge the open Dependabot updates for NymBroker (NuGet packages and GitHub Actions) in one batch — collect the branches, merge them on one branch, fix what the new versions break, test, and land on master. Use when the user asks to merge, update, bump or clean up dependencies, Dependabot pull requests or dependabot branches.
---

# Merge Dependabot updates

Dependabot runs weekly (`.github/dependabot.yml`) for NuGet and GitHub Actions. The .NET servicing packages (`Microsoft.Extensions.*`, `Microsoft.Data.Sqlite`, `Microsoft.AspNetCore.*`, `Microsoft.EntityFrameworkCore.*`, `System.Security.Cryptography.Pkcs`) arrive as one group, `dotnet-servicing`. Package versions are set in each `.csproj` (there is no `Directory.Packages.props`), so one package can be referenced in several projects.

## Steps

1. **List the updates.**
   ```bash
   git fetch --prune origin
   gh pr list --repo nymankla/NymBroker --author "app/dependabot" --json number,title,headRefName
   git branch -r --no-merged master | rg dependabot
   ```
   Some updates exist only as branches without an open pull request. Include those too.
2. **Tell the user what you will merge** and point out every major version bump. A major bump of a database driver or broker client (`Npgsql`, `Microsoft.Data.SqlClient`, `MySqlConnector`, `RabbitMQ.Client`, `Azure.Messaging.ServiceBus`, `AWSSDK.SQS`) can change behaviour: ask before you include it.
3. **Merge on one branch.** `git switch -c deps/merge-<yyyy-mm-dd> master`, then `git merge --no-ff origin/<branch>` for each update, smallest first and the `dotnet-servicing` group last. For a conflict in a `.csproj`, keep the higher version.
4. **Make the versions equal.** After the merges, each package must have the same version in every project:
   ```bash
   rg -n "PackageReference Include" --glob "*.csproj" . | rg "<package name>"
   ```
5. **Build: 0 warnings.** New analyzer versions add warnings. Fix the code. Do not suppress the warning and do not lower a severity.
6. **Test.** `dotnet test` always. `./scripts/test-all.ps1` when a driver, broker client or `Microsoft.Data.Sqlite` changed. Run the self-checking samples that use the changed package (list in the root `CLAUDE.md`).
7. **Pack** when a packable project changed: `./scripts/pack.ps1`. CI requires one `.snupkg` for each `.nupkg`.
8. **Changelog.** Under `[Unreleased]` → Changed, one line in the existing form: ``- Dependencies: `Microsoft.Extensions.*` 11.0.1, `AWSSDK.SQS` 4.1.0.`` List only packages that ship in a NuGet package. Leave out test, sample and GitHub Actions updates.
9. **Land.** Merge the branch into `master` with `--no-ff`. Put what the updates required in the merge commit message (for example which analyzer rule made you change code). Push only when the user asks. GitHub closes the Dependabot pull requests when their commits reach `master`; delete remote branches that remain.

## Do not

- Do not change `<Version>` in `Directory.Build.props`. A dependency update alone is not a release.
- Do not move a Docker image to a new major version in `scripts/docker-compose.yml` as part of this: the data volumes depend on the current major version.
