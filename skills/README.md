# NymBroker skills for AI coding agents

Agent skills that help an AI coding agent (Claude Code, or any tool that reads `SKILL.md` skill folders) build an application **on top of** NymBroker. Install them in your client project, not in this repository.

| Skill | What it does |
|---|---|
| [nymbroker-setup](nymbroker-setup/SKILL.md) | Asks which transport, role (producer / consumer), configuration style and extras you want, then installs the NuGet packages and registers the broker |
| [nymbroker-message](nymbroker-message/SKILL.md) | Creates a message type with a stable `[MessageName]` and shows how to send it |
| [nymbroker-consumer](nymbroker-consumer/SKILL.md) | Creates an `IConsume<T>` consumer or `ISubscribe<T>` topic subscribers, registers them, and keeps them safe under retries |
| [nymbroker-routing](nymbroker-routing/SKILL.md) | Routes and topics: forward or fan out messages by type, source or content, with loop guards |
| [nymbroker-producer-worker](nymbroker-producer-worker/SKILL.md) | Splits an app into a `WriteOnly` producer and scalable worker services sharing a durable queue |
| [nymbroker-input-transformer](nymbroker-input-transformer/SKILL.md) | Reads CSV, XML, plain text or foreign JSON into typed messages |
| [nymbroker-scheduled](nymbroker-scheduled/SKILL.md) | Interval and cron scheduled actions, preferably posting a trigger message to a consumer |
| [nymbroker-testing](nymbroker-testing/SKILL.md) | Unit and integration tests for consumers, routes and retries, without flaky delays |
| [nymbroker-observability](nymbroker-observability/SKILL.md) | OpenTelemetry metrics and traces, health checks and probes, logging, alerts |
| [nymbroker-dead-letters](nymbroker-dead-letters/SKILL.md) | Finds, inspects and replays failed messages for each transport |
| [nymbroker-troubleshoot](nymbroker-troubleshoot/SKILL.md) | Diagnoses messages not consumed, loops, stuck or failed rows, startup errors and more |

## Install

Download **[nymbroker-skills.zip](https://github.com/nymankla/NymBroker/releases/latest/download/nymbroker-skills.zip)** from the latest release and extract it into your project's `.claude/skills/` folder (or `~/.claude/skills/` to use them in all your projects):

```powershell
# PowerShell, from your project root
Invoke-WebRequest https://github.com/nymankla/NymBroker/releases/latest/download/nymbroker-skills.zip -OutFile nymbroker-skills.zip
Expand-Archive nymbroker-skills.zip -DestinationPath .claude/skills -Force
Remove-Item nymbroker-skills.zip
```

```bash
# bash, from your project root
curl -L -o nymbroker-skills.zip https://github.com/nymankla/NymBroker/releases/latest/download/nymbroker-skills.zip
mkdir -p .claude/skills && unzip -o nymbroker-skills.zip -d .claude/skills && rm nymbroker-skills.zip
```

You should end up with one folder per skill, such as `.claude/skills/nymbroker-setup/SKILL.md`. Commit them so everyone on the team gets them.

## Use

Ask in plain words — the agent picks the matching skill — or invoke one directly in Claude Code:

- "Add NymBroker to this app" or `/nymbroker-setup`
- "Create an OrderCreated message" or `/nymbroker-message`
- "Add a consumer that stores OrderCreated in the database" or `/nymbroker-consumer`
- "Send high-priority orders to a separate queue" or `/nymbroker-routing`
- "Why isn't my consumer called?" or `/nymbroker-troubleshoot`

The skills match the NymBroker version of the release they were downloaded from. Download them again when you upgrade the packages.

The zip is built by `scripts/pack.ps1` (written to `artifacts/nymbroker-skills.zip`) and attached to every GitHub release by CI.
