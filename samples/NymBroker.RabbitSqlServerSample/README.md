# RabbitMQ with SQL Server for idempotency and scheduling

Messages travel through RabbitMQ. SQL Server holds what the workers have to agree on: which messages have been handled, and which scheduled runs have happened. Workers share the queue and both tables, so you can run several, on one machine or many.

| Concern | Package | What it adds |
|---|---|---|
| Transport | `NymBroker.Endpoint.RabbitMq` | Workers compete for messages on one queue |
| Duplicate detection | `NymBroker.Idempotency.SqlServer` | A message id seen before isn't handled again, on any worker, also after a restart |
| Schedule | `NymBroker.Scheduling.SqlServer` | Each occurrence of a named action runs on one worker, and a run missed while all were down is caught up |

## Run it

Start the two services, from the repository root:

```bash
./scripts/setup-rabbitmq.ps1
./scripts/setup-sqlserver.ps1
```

Run the self-checking demo, with two workers in one process. It takes about 25 seconds and exits with 0 when every check holds:

```bash
dotnet run --project samples/NymBroker.RabbitSqlServerSample -- --demo
```

Or run one worker per terminal, and watch them share the schedule:

```bash
dotnet run --project samples/NymBroker.RabbitSqlServerSample
```

## What the demo checks

1. **One run per occurrence.** Both workers schedule `invoice-run` every 5 seconds, and each occurrence runs on one of them.
2. **Duplicates.** The same envelope posted twice is handled once.
3. **Failures aren't remembered as done.** A consumer that throws releases its claim, so RabbitMQ's redelivery runs, and the message is handled once.
4. **Restarts.** With all workers stopped, a run is missed, and it's caught up as soon as a worker starts. The duplicate from step 2 is still recognised.

## Before production

- The connection settings here are for the local containers. See [Production security](../../docs/security.md) for TLS, secrets and table permissions.
- Choose `Ttl` to be longer than a duplicate can arrive after its original; the sample uses one hour.
- A consumer's own writes and its idempotency claim are separate transactions. See [Reliability](../../docs/reliability.md).
