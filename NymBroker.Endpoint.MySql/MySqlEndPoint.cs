using System.Text.Json;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Endpoint.MySql;

/// <summary>
/// MySQL 8.0+ / MariaDB 10.6+ queue endpoint. Same lifecycle as the PostgreSQL and SQL Server endpoints: rows are
/// claimed with a lease (<c>FOR UPDATE SKIP LOCKED</c>, so several instances can poll one table), dispatched, then
/// marked Completed, returned to Pending, or marked Failed after <see cref="MySqlSettings.MaxRetryCount"/>. The loop
/// itself is the shared <see cref="LeasedQueueListener"/>.
/// <para>
/// MySQL has no <c>UPDATE … RETURNING</c>, so a claim is two round trips in one transaction: finalize the previous
/// batch + lock the next ids, then claim + read + commit. Polls on a timer when idle.
/// </para>
/// </summary>
public sealed class MySqlEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private static readonly JsonSerializerOptions FinalizeJsonOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private readonly MySqlSettings _settings;
    private readonly ILogger<MySqlEndPoint> _logger;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private readonly string _name;
    private readonly string _table;
    private readonly Listener _listener;

    private volatile bool _schemaEnsured;

    public EndpointMode Mode { get; }

    public MySqlEndPoint(string name, MySqlSettings settings, ILogger<MySqlEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        Mode = mode;
        _settings = settings;
        _logger = logger;
        _table = MySqlQueueSql.QuoteTableName(settings.TableName);
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
        await using var cmd = new MySqlCommand(MySqlQueueSql.InsertMessage(_table), conn);
        cmd.Parameters.AddWithValue("@messageId", Guid.NewGuid().ToByteArray());
        cmd.Parameters.AddWithValue("@payload", message);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>All messages in one multi-row INSERT (one round trip, one commit); atomic, order preserved.</summary>
    public async Task PostBatchAsync(IReadOnlyList<byte[]> messages, CancellationToken ct = default)
    {
        if (messages.Count == 0) return;

        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = new MySqlCommand(MySqlQueueSql.InsertMessages(_table, messages.Count), conn);
        for (var i = 0; i < messages.Count; i++)
        {
            cmd.Parameters.AddWithValue($"@m{i}", Guid.NewGuid().ToByteArray());
            cmd.Parameters.AddWithValue($"@p{i}", messages[i]);
        }
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public IHealthCheckResult HealthCheck()
        => HealthCheckResult.FromProbe("MySQL", _name, _logger, async ct =>
        {
            await using var conn = await OpenConnectionAsync(ct);
            await using var cmd = new MySqlCommand("SELECT 1", conn);
            _ = await cmd.ExecuteScalarAsync(ct);
        });

    public async ValueTask DisposeAsync()
    {
        await StopListeningAsync();
        _schemaLock.Dispose();
    }

    private async Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        try
        {
            var ids = new List<long>();
            await using (var lockCmd = new MySqlCommand(MySqlQueueSql.FinalizeAndLock(_table), conn))
            {
                AddItemsParameter(lockCmd, outcomes);
                lockCmd.Parameters.AddWithValue("@batchSize", _settings.BatchSize);
                await using var reader = await lockCmd.ExecuteReaderAsync(ct);
                do
                {
                    while (await reader.ReadAsync(ct))
                        ids.Add(reader.GetInt64(0));
                } while (await reader.NextResultAsync(ct));
            }

            if (ids.Count == 0)
            {
                await using var commit = new MySqlCommand("COMMIT;", conn);
                await commit.ExecuteNonQueryAsync(ct);
                return [];
            }

            var claimed = new List<QueueMessage>(ids.Count);
            await using (var claimCmd = new MySqlCommand(MySqlQueueSql.Claim(_table, ids), conn))
            {
                claimCmd.Parameters.AddWithValue("@leaseTimeout", LeasedQueueListener.GetLeaseTimeoutSeconds(_settings));
                await using var reader = await claimCmd.ExecuteReaderAsync(ct);
                do
                {
                    while (await reader.ReadAsync(ct))
                    {
                        claimed.Add(new QueueMessage(
                            QueueId: reader.GetInt64(0),
                            AttemptCount: reader.GetInt32(3),
                            Payload: reader.GetFieldValue<byte[]>(2),
                            MessageId: new Guid(reader.GetFieldValue<byte[]>(1))));
                    }
                } while (await reader.NextResultAsync(ct));   // drain, so a COMMIT error surfaces here
            }
            return claimed;
        }
        catch
        {
            // The connection goes back to the pool: don't leave the transaction (and its row locks) open until reuse.
            await RollbackQuietlyAsync(conn);
            throw;
        }
    }

    private async Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = new MySqlCommand(MySqlQueueSql.Finalize(_table), conn);
        AddItemsParameter(cmd, outcomes);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static void AddItemsParameter(MySqlCommand cmd, IReadOnlyList<QueueMessageOutcome> outcomes)
    {
        var items = outcomes.Count == 0
            ? (object)DBNull.Value
            : JsonSerializer.Serialize(
                outcomes.Select(static o => new FinalizeItem(o.Message.QueueId, o.Message.AttemptCount, (int)o.Status, o.Error)),
                FinalizeJsonOptions);
        cmd.Parameters.AddWithValue("@items", items);
    }

    private async Task RollbackQuietlyAsync(MySqlConnection conn)
    {
        try
        {
            await using var rollback = new MySqlCommand("ROLLBACK;", conn);
            await rollback.ExecuteNonQueryAsync(CancellationToken.None);
        }
        catch (Exception ex)
        {
            // The original exception is rethrown and logged by the listener; this only explains the follow-up.
            _logger.LogWarning(ex, "Could not roll back the claim transaction on endpoint '{Name}'; the server releases it when the connection closes", _name);
        }
    }

    private async Task<MySqlConnection> OpenConnectionAsync(CancellationToken ct)
    {
        // MySqlConnector pools connections per connection string, so one connection per operation is cheap
        // and keeps PostAsync safe to call concurrently (MySqlConnection itself is not thread-safe).
        var conn = new MySqlConnection(_settings.ConnectionString);
        try
        {
            await conn.OpenAsync(ct);
            if (_settings.AutoCreateTable && !_schemaEnsured)
                await EnsureSchemaAsync(conn, ct);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    private async Task EnsureSchemaAsync(MySqlConnection conn, CancellationToken ct)
    {
        await _schemaLock.WaitAsync(ct);
        try
        {
            if (_schemaEnsured) return;
            await using var cmd = new MySqlCommand(MySqlQueueSql.CreateSchema(_table), conn);
            await cmd.ExecuteNonQueryAsync(ct);
            _schemaEnsured = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    private sealed record FinalizeItem(long Id, int Attempt, int Status, string? Error);

    /// <summary>The shared queue loop over this endpoint's finalize-and-claim statements.</summary>
    private sealed class Listener(MySqlEndPoint owner) : LeasedQueueListener(owner._name, owner._settings, owner._logger)
    {
        protected override Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
            => owner.FinalizeAndClaimAsync(outcomes, ct);

        protected override Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
            => owner.FinalizeAsync(outcomes, ct);
    }
}
