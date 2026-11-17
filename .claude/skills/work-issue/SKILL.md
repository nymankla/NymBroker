---
name: work-issue
description: Work a NymBroker GitHub issue from start to finish — read it, confirm the cause in the code, post an implementation plan as an issue comment, then (when the user says so) implement it against the definition of done. Use when the user gives an issue number or URL and asks to go through, review, plan, comment on, fix or implement it.
---

# Work an issue

Do the steps in order. Stop after step 3 unless the user asked for the implementation too.

## 1. Read the issue

```bash
gh issue view <n> --repo nymankla/NymBroker --json number,title,state,body,labels,comments,author
```

Use `--json`: the plain `gh issue view <n> --comments` prints nothing in this environment. Read the existing comments too, since a plan may already be there.

## 2. Confirm the cause in the code

Do not repeat the issue's analysis as fact. Check it:

- Read the code the issue names, and the `CLAUDE.md` of that project's folder.
- For a bug, find the exact line or generated code that causes it. If the issue's explanation is incomplete or wrong, say so in the plan.
- For source generators (request delegate generator, JSON), read the generated code: build the project with `-p:EmitCompilerGeneratedFiles=true -p:CompilerGeneratedFilesOutputPath=<scratchpad dir>`.
- Check whether routes, endpoints or stores that are not named in the issue have the same problem.

## 3. Post the plan as an issue comment

Write it in English, with `gh issue comment <n> --repo nymankla/NymBroker --body-file <file>`. Sections:

1. **Cause, confirmed**: what you verified and how, and anything the issue missed.
2. **Fix**: the change, file by file. Say whether the public API changes.
3. **Why not the alternatives**: one line for each option from the issue that you did not choose.
4. **Tests**: the regression test (for a bug it must fail without the fix) and any guard test that catches the same mistake in new code.
5. **Other changes**: `CHANGELOG.md`, `docs/`, `skills/*/SKILL.md`, the folder `CLAUDE.md`, samples.
6. **Release**: which version level fits. The level is the user's decision.

Ask the user first if the fix needs an architectural decision (new project, public API shape, new dependency).

Then give the user the comment link and a short summary. If the cause is a defect in another project (for example dotnet/aspnetcore), say so, but report it there only when the user asks. An upstream report needs a standalone repro that you have run and must not mention NymBroker.

## 4. Implement (when the user asks)

- Follow the plan. If you must leave it, say why in the commit message or in a new issue comment.
- Complete every item of the "Definition of done" in the root `CLAUDE.md`.
- Commit message: explain why, end with `Fixes #<n>`.
- After the push, tell the user what the next release needs (changelog, PublicAPI, version level).
