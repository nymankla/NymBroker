# Production security

[← User guide](user-guide.md)

Out of the box, NymBroker is set up for the local Docker containers in `scripts/`: plaintext RabbitMQ, a SQL Server connection that skips certificate verification, and well-known passwords. Before production, every transport needs an encrypted, verified connection, a least-privilege identity, and secrets from a secret store. This page shows how for each endpoint, and what else can leak.

> **The defaults are for local development only.** `SqlServerSettings` and `SqlServerIdempotencySettings` default to `Server=localhost,1433;…;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True`, `PostgresSettings` to `…Username=postgres;Password=postgres`, and `RabbitMqSettings` to `guest` / `guest` without TLS. Always set the connection settings explicitly in production; never rely on a default.

## Checklist

- [ ] Every connection is encrypted **and** verifies the server certificate (no `TrustServerCertificate=True`, no SSL mode below `VerifyFull`, `UseTls = true` for RabbitMQ).
- [ ] Secrets come from a secret store or a managed identity, not from `queuesettings.json` or source control.
- [ ] Each application identity has only the permissions it needs; the queue tables are created by a deployment step, not by the app (`AutoCreateTable = false`).
- [ ] Only trusted producers can write to the queues the broker consumes.
- [ ] Payload logging (`AddMessageLoggingFilter`) is off, and File dead-letter / wire-tap folders are access-controlled.
- [ ] The health endpoint is not public.

## Secrets

- Keep connection strings and passwords out of `queuesettings.json` and the repository. Read endpoint configuration from `IConfiguration` so secrets can come from environment variables, [user secrets](https://learn.microsoft.com/aspnet/core/security/app-secrets) during development, or Azure Key Vault / your platform's secret store in production:

  ```csharp
  builder.Services.AddNymBroker()
      .ApplyConfiguration(BrokerConfigurationReader.Read(builder.Configuration))   // section "NymBroker"
      .WithSqlServer()
      .Build();
  ```

  With the default configuration providers, an environment variable such as `NymBroker__Endpoints__0__Config__connectionString` overrides the value from `appsettings.json`.
- Prefer identities over passwords where the service supports them: a managed identity for Azure Service Bus and Azure SQL (below), Windows integrated authentication for on-premises SQL Server.
- A `TokenCredential` can only be set in code (`AzureServiceBusSettings.Credential` is `[JsonIgnore]`), so it never ends up in a config file.

## RabbitMQ

Turn on TLS and use the broker's TLS port:

```csharp
.AddRabbitMqEndPoint("Orders", new RabbitMqSettings
{
    HostName        = "rabbit.prod.example.com",
    Port            = 5671,                                  // AMQPS
    UseTls          = true,
    User            = "orders-service",
    Password        = builder.Configuration["Rabbit:Password"],
    VirtualHost     = "orders",
    ReadQueueName   = "orders.in"
})
```

| Setting | Meaning |
|---|---|
| `UseTls` | Connect over TLS. The server certificate is **always verified**: it must chain to a CA the machine trusts, be valid, and match the server name. There is no setting to turn verification off. |
| `TlsServerName` | The name the certificate must match. Defaults to `HostName`; set it when you connect by IP address or through an alias that isn't in the certificate. |
| `ClientCertificatePath`, `ClientCertificatePassword` | A PKCS#12 (`.pfx`) client certificate for mutual TLS, when the broker requires one. Keep the password in the secret store. |

- **Certificate trust:** if the broker's certificate comes from a private CA, install that CA in the machine's trust store (on Linux containers: copy it to `/usr/local/share/ca-certificates/` and run `update-ca-certificates` in the image). Don't work around a trust error by weakening verification.
- **Identity:** create a user per application with permissions only on its own virtual host and queues. RabbitMQ's built-in `guest` user only works from localhost, so the default settings fail against a remote broker by design.
- **Dead letters:** configure a dead-letter exchange on each consumed queue; otherwise rejected messages are dropped (see [Reliability](reliability.md)).
- TLS on the broker side: [RabbitMQ TLS support](https://www.rabbitmq.com/docs/ssl).

## SQL Server

Microsoft.Data.SqlClient (7.x in NymBroker) encrypts by default (`Encrypt=Mandatory`) and verifies the server certificate — unless the connection string turns verification off. The development default does exactly that with `TrustServerCertificate=True`; **never deploy it**. It keeps the traffic encrypted but stops the client from checking *who* it is talking to, so a machine in the middle can read and change every message.

```text
Server=tcp:sql.prod.example.com,1433;Database=orders;User ID=nymbroker_orders;Password=<from secret store>;Encrypt=Mandatory;TrustServerCertificate=False
```

| Keyword | Production value |
|---|---|
| `Encrypt` | `Mandatory` (the default) or `Strict` (TDS 8.0, SQL Server 2022 and Azure SQL; the certificate is always verified and `TrustServerCertificate` has no effect). Never `False` / `Optional`. |
| `TrustServerCertificate` | `False` (the default). Fix a certificate error by fixing the certificate, not with this flag. |
| `HostNameInCertificate` | Only when you connect through an alias that isn't in the certificate's subject alternative names. It changes the expected name; it does not skip the trust or expiry checks. |
| `ServerCertificate` | Optional pinning: the path of the expected server certificate (`.cer`, PEM or DER). Must be updated on every certificate renewal. |

**Certificate:** the server certificate must include the name clients connect with in its subject alternative names, and its issuing CA must be trusted on every client host and container. An enterprise CA is not automatically trusted inside a Linux container.

**Identity:**

- Azure SQL with a managed identity — no password at all:

  ```text
  Server=tcp:<server>.database.windows.net,1433;Database=orders;Authentication=Active Directory Managed Identity;Encrypt=Strict
  ```

  SqlClient 7 moved the built-in Microsoft Entra ID modes into a separate package: add `Microsoft.Data.SqlClient.Extensions.Azure` at the same version as `Microsoft.Data.SqlClient` to your application.
- On-premises: Windows integrated authentication (`Integrated Security=true`) or a SQL login whose password comes from the secret store. Never `sa`.

**Permissions:** create the queue table (and the idempotency table) in a deployment step with a privileged identity, then run the application with `AutoCreateTable = false` and only these rights:

```sql
GRANT SELECT, INSERT, UPDATE ON dbo.orders_queue TO nymbroker_orders;
GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.nymbroker_idempotency TO nymbroker_orders;   -- only with AddSqlServerIdempotency
```

Producers that only post need `INSERT`. With `AutoCreateTable = true` the identity additionally needs `CREATE TABLE` / `ALTER` rights on the schema.

References: [Encryption and certificate validation](https://learn.microsoft.com/sql/connect/ado-net/encryption-and-certificate-validation), [Microsoft Entra authentication with SqlClient](https://learn.microsoft.com/sql/connect/ado-net/sql/azure-active-directory-authentication), [SqlClient security best practices](https://learn.microsoft.com/sql/connect/ado-net/sql/application-security-scenarios-sql-server).

## PostgreSQL

Npgsql's default `SSL Mode=Prefer` neither requires encryption nor verifies the certificate, and `Require` encrypts without verifying. Use `VerifyFull`:

```text
Host=pg.prod.example.com;Database=orders;Username=nymbroker_orders;Password=<from secret store>;SSL Mode=VerifyFull
```

- `VerifyFull` requires TLS and checks both the certificate chain and the host name. If the server's CA is not in the machine's trust store, point to it with `Root Certificate=/path/ca.crt` (or the `PGSSLROOTCERT` environment variable).
- Client certificates: `SSL Certificate`, `SSL Key` and `SSL Password`.
- **Permissions:** as for SQL Server — create the tables in a deployment step, run with `AutoCreateTable = false`, and grant `SELECT, INSERT, UPDATE` on the queue table (plus `DELETE` on the idempotency table). `LISTEN` / `NOTIFY` (`UseNotifications`) need no extra grant.

Reference: [Npgsql security and encryption](https://www.npgsql.org/doc/security.html).

## Azure Service Bus

Connections always use TLS with certificate verification; nothing to configure. What matters is the credential:

- **Prefer a managed identity** over a connection string:

  ```csharp
  .AddAzureServiceBusEndPoint("Orders", new AzureServiceBusSettings
  {
      FullyQualifiedNamespace = "orders-prod.servicebus.windows.net",
      Credential              = new DefaultAzureCredential(),   // Azure.Identity, referenced by your app
      QueueName               = "orders"
  })
  ```

  Assign the identity the **Azure Service Bus Data Sender** and/or **Data Receiver** role on the queue or topic, not the whole namespace.
- If you must use a connection string, use a shared access policy scoped to the entity with only `Send` or `Listen` rights — never `RootManageSharedAccessKey` (the emulator's key) or `Manage`.

## SQLite

SQLite has no network or authentication: whoever can open the file can read and change the queue.

- Put the database file in a directory only the application's account can read and write; never on a network share.
- The payloads are stored unencrypted. Use an encrypted disk if the messages contain personal or confidential data.

## What else can leak

| Where | What | Protect by |
|---|---|---|
| `AddMessageLoggingFilter()` | Logs every message **with its payload** at `Debug` | Don't enable it in production, or make sure `Debug` logs of category `NymBroker` aren't collected |
| File endpoints used as dead-letter or wire-tap targets | Full message envelopes, written as JSON files | Restrict the folder's permissions; clean it up on a schedule |
| SQL queue tables | Payloads, plus exception messages in `last_error` | Database permissions; delete old `Completed` / `Failed` rows |
| Health endpoint (`AddNymBroker` health check) | Endpoint names and connection error messages | Don't expose `/health` publicly; map it on an internal port or require authorization |
| Metrics and traces | Endpoint, topic, message-type and consumer names — never payloads | Nothing extra; they are safe to export |

## Untrusted producers

The broker processes whatever arrives on the endpoints it listens to. Anyone who can write to a queue can make consumers run, and can send split-message parts that never complete — the broker keeps incomplete parts in memory for up to two hours. Restrict write access to the transport (database permissions, RabbitMQ and Service Bus rights) to trusted producers, and validate message contents in consumers like any other input.
