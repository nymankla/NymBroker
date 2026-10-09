using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Endpoint.SqlServer;

/// <summary>
/// SQL Server queue endpoint. Same lifecycle as <c>PostgresEndPoint</c>: rows are claimed with a lease
/// (<c>UPDLOCK, READPAST</c>, so several instances can poll one table), dispatched, then marked
/// Completed, returned to Pending, or marked Failed after <see cref="SqlServerSettings.MaxRetryCount"/>. The loop itself is the
/// shared <see cref="LeasedQueueListener"/>.
/// <para>
/// SQL Server specifics: a batch's results are written in the same statement and transaction that claims the
/// next batch (one commit per batch), and a filtered index keeps claiming cheap as completed rows accumulate.
/// Polls on a timer when idle — SQL Server has no lightweight LISTEN/NOTIFY equivalent.
/// </para>
/// </summary>
public sealed class SqlServerEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private static readonly JsonSerializerOptions FinalizeJsonOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private readonly SqlServerSettings _settings;
    private readonly ILogger<SqlServerEndPoint> _logger;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private readonly string _name;
    private readonly Listener _listener;

    private bool _schemaEnsured;

    public EndpointMode Mode { get; }

    public SqlServerEndPoint(string name, SqlServerSettings settings, ILogger<SqlServerEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        Mode = mode;
        _settings = settings;
        _logger = logger;
        _listener = new Listener(this);
    }

    /// <summary>Dead-letters by marking the row <c>Failed</c> immediately, with the reason in <c>last_error</c>.</summary>
    public bool UsesNativeDeadLetter => _settings.UseNativeDeadLetter;

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        _listener.Start(handler, ct);
        return Task.CompletedTask;
    }

    /// <summary>Stops polling and waits for the loop to finish, so no handler runs after this returns.</summary>
    public Task StopListeningAsync() => _listener.StopAsync();

    public async Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = SqlServerQueueSql.InsertMessage(_settings.TableName);
        cmd.Parameters.Add("@messageId", SqlDbType.UniqueIdentifier).Value = Guid.NewGuid();
        // Explicit VARBINARY(MAX) — an inferred size would create one cached plan per payload length.
        cmd.Parameters.Add("@payload", SqlDbType.VarBinary, -1).Value = message;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>All messages in one INSERT ... SELECT FROM OPENJSON (one round trip, one commit); atomic, order preserved.</summary>
    public async Task PostBatchAsync(IReadOnlyList<byte[]> messages, CancellationToken ct = default)
    {
        if (messages.Count == 0) return;

        // System.Text.Json writes byte[] as base64, which OPENJSON decodes back into VARBINARY.
        var items = JsonSerializer.Serialize(messages.Select(static (payload, index) => new BatchItem(index, payload)), FinalizeJsonOptions);

        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = SqlServerQueueSql.InsertMessages(_settings.TableName);
        cmd.Parameters.Add("@items", SqlDbType.NVarChar, -1).Value = items;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public IHealthCheckResult HealthCheck()
        => HealthCheckResult.FromProbe("SQL Server", _name, _logger, async ct =>
        {
            await using var conn = await OpenConnectionAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            _ = await cmd.ExecuteScalarAsync(ct);
        });

    public async ValueTask DisposeAsync()
    {
        await StopListeningAsync();
        _schemaLock.Dispose();
    }

    private async Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
    {
        // The transaction lives inside the T-SQL batch (BEGIN/COMMIT + XACT_ABORT), so finalize + claim + commit
        // is a single round trip instead of three (BeginTransaction, execute, Commit).
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = SqlServerQueueSql.FinalizeAndClaim(_settings.TableName);
        AddItemsParameter(cmd, outcomes);
        cmd.Parameters.Add("@batchSize", SqlDbType.Int).Value = _settings.BatchSize;
        cmd.Parameters.Add("@leaseTimeout", SqlDbType.Int).Value = LeasedQueueListener.GetLeaseTimeoutSeconds(_settings);

        var claimed = new List<QueueMessage>();
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                claimed.Add(new QueueMessage(
                    QueueId: reader.GetInt64(0),
                    AttemptCount: reader.GetInt32(3),
                    Payload: reader.GetFieldValue<byte[]>(2),
                    MessageId: reader.GetGuid(1)));
            }

            // Drain remaining results so a COMMIT error (rare) surfaces here instead of being lost.
            while (await reader.NextResultAsync(ct)) { }
        }

        return claimed;
    }

    private async Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = SqlServerQueueSql.Finalize(_settings.TableName);
        AddItemsParameter(cmd, outcomes);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static void AddItemsParameter(SqlCommand cmd, IReadOnlyList<QueueMessageOutcome> outcomes)
    {
        var items = outcomes.Count == 0
            ? (object)DBNull.Value
            : JsonSerializer.Serialize(
                outcomes.Select(static o => new FinalizeItem(o.Message.QueueId, o.Message.AttemptCount, (int)o.Status, o.Error)),
                FinalizeJsonOptions);
        cmd.Parameters.Add("@items", SqlDbType.NVarChar, -1).Value = items;
    }

    private async Task<SqlConnection> OpenConnectionAsync(CancellationToken ct)
    {
        // SqlClient pools connections per connection string, so one connection per operation is cheap
        // and keeps PostAsync safe to call concurrently (SqlConnection itself is not thread-safe).
        var conn = new SqlConnection(_settings.ConnectionString);
        try
        {
            await conn.OpenAsync(ct);
            if (_settings.AutoCreateTable)
                await EnsureSchemaAsync(conn, ct);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    private async Task EnsureSchemaAsync(SqlConnection conn, CancellationToken ct)
    {
        if (_schemaEnsured) return;

        await _schemaLock.WaitAsync(ct);
        try
        {
            if (_schemaEnsured) return;

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = SqlServerQueueSql.CreateSchema(_settings.TableName);
            await cmd.ExecuteNonQueryAsync(ct);
            _schemaEnsured = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    private sealed record BatchItem(int I, byte[] P);

    private sealed record FinalizeItem(long Id, int Attempt, int Status, string? Error);

    /// <summary>The shared queue loop over this endpoint's finalize-and-claim statements.</summary>
    private sealed class Listener(SqlServerEndPoint owner) : LeasedQueueListener(owner._name, owner._settings, owner._logger)
    {
        protected override Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
            => owner.FinalizeAndClaimAsync(outcomes, ct);

        protected override Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
            => owner.FinalizeAsync(outcomes, ct);
    }
}
