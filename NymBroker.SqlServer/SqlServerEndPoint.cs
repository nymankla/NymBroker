using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.SqlServer;

/// <summary>
/// SQL Server queue endpoint. Same lifecycle as <c>PostgresEndPoint</c>: rows are claimed with a lease
/// (<c>UPDLOCK, READPAST</c>, so several instances can poll one table), dispatched, then marked
/// Completed, returned to Pending, or marked Failed after <see cref="SqlServerSettings.MaxRetryCount"/>.
/// <para>
/// SQL Server specifics: a batch's results are written in the same statement and transaction that claims the
/// next batch (one commit per batch), and a filtered index keeps claiming cheap as completed rows accumulate.
/// Polls on a timer when idle — SQL Server has no lightweight LISTEN/NOTIFY equivalent.
/// </para>
/// </summary>
public sealed class SqlServerEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private static readonly TimeSpan ShutdownFinalizeTimeout = TimeSpan.FromSeconds(10);
    private static readonly JsonSerializerOptions FinalizeJsonOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private readonly SqlServerSettings _settings;
    private readonly ILogger<SqlServerEndPoint> _logger;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private readonly string _name;

    private bool _schemaEnsured;
    private CancellationTokenSource? _listeningCts;
    private Task? _loop;

    public EndpointMode Mode { get; }

    public SqlServerEndPoint(string name, SqlServerSettings settings, ILogger<SqlServerEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        Mode = mode;
        _settings = settings;
        _logger = logger;
    }

    /// <summary>Dead-letters by marking the row <c>Failed</c> immediately, with the reason in <c>last_error</c>.</summary>
    public bool UsesNativeDeadLetter => _settings.UseNativeDeadLetter;

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;

        _loop = Task.Run(() => RunListenerLoopAsync(handler, token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopListeningAsync()
    {
        _listeningCts?.Cancel();
        if (_loop is not null)
        {
            await _loop;
            _loop = null;
        }
    }

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
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            return HealthCheckAsync(cts.Token).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SQL Server endpoint '{Name}' health check failed", _name);
            return HealthCheckResult.Unhealthy(ex.Message);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopListeningAsync();
        _listeningCts?.Dispose();
        _listeningCts = null;
        _schemaLock.Dispose();
    }

    private async Task<IHealthCheckResult> HealthCheckAsync(CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        _ = await cmd.ExecuteScalarAsync(ct);
        return HealthCheckResult.Healthy();
    }

    private async Task RunListenerLoopAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken token)
    {
        // Results of the last processed batch, written by the next FinalizeAndClaim round trip.
        var unfinalized = new List<MessageCompletion>();
        try
        {
            while (!token.IsCancellationRequested)
            {
                var claimedCount = 0;
                try
                {
                    var messages = await FinalizeAndClaimAsync(unfinalized, token);
                    unfinalized.Clear();   // committed together with the claim
                    claimedCount = messages.Count;
                    await ProcessClaimedMessagesAsync(messages, handler, unfinalized, token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // The transaction rolled back, so 'unfinalized' is retried on the next cycle. If the leases
                    // expire first, the attempt_count guard turns that late finalize into a no-op.
                    _logger.LogError(ex, "Poll error on endpoint '{Name}'", _name);
                }

                // Drain without delay while there is work; poll on the interval when idle.
                if (claimedCount == 0)
                    await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(1, _settings.PollInterval.TotalMilliseconds)), token);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", _name);
        }

        await FinalizeOnShutdownAsync(unfinalized);
    }

    private async Task ProcessClaimedMessagesAsync(List<ClaimedMessage> messages, Func<byte[], CancellationToken, Task<ProcessResult>> handler,
        List<MessageCompletion> completions, CancellationToken ct)
    {
        foreach (var message in messages)
        {
            ProcessResult result;
            try
            {
                result = await handler(message.Payload, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Unhandled error dispatching message on endpoint '{Name}'", _name);
                result = ProcessResult.Retry(ex);
            }
            completions.Add(ToCompletion(message, result));
        }
    }

    private MessageCompletion ToCompletion(ClaimedMessage message, ProcessResult result)
    {
        switch (result.Outcome)
        {
            case ProcessOutcome.Completed:
                return MessageCompletion.Completed(message);

            case ProcessOutcome.DeadLetter:
                _logger.LogWarning("Message {MessageId} on endpoint '{Name}' dead-lettered (marked Failed): {Failure}",
                    message.MessageId, _name, result.FailureText);
                return MessageCompletion.FromFailure(message, MessageStatus.Failed, result.FailureText);

            default:
                var terminal = message.AttemptCount >= _settings.MaxRetryCount;
                if (terminal)
                    _logger.LogWarning("Message {MessageId} on endpoint '{Name}' marked Failed after {Attempts} attempts (terminal state); last error: {Error}",
                        message.MessageId, _name, message.AttemptCount, result.FailureText);
                return MessageCompletion.FromFailure(message, terminal ? MessageStatus.Failed : MessageStatus.Pending, result.FailureText);
        }
    }

    private async Task<List<ClaimedMessage>> FinalizeAndClaimAsync(List<MessageCompletion> unfinalized, CancellationToken ct)
    {
        // The transaction lives inside the T-SQL batch (BEGIN/COMMIT + XACT_ABORT), so finalize + claim + commit
        // is a single round trip instead of three (BeginTransaction, execute, Commit).
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = SqlServerQueueSql.FinalizeAndClaim(_settings.TableName);
        AddItemsParameter(cmd, unfinalized);
        cmd.Parameters.Add("@batchSize", SqlDbType.Int).Value = _settings.BatchSize;
        cmd.Parameters.Add("@leaseTimeout", SqlDbType.Int).Value = GetLeaseTimeoutSeconds();

        var claimed = new List<ClaimedMessage>();
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                claimed.Add(new ClaimedMessage
                {
                    QueueId = reader.GetInt64(0),
                    MessageId = reader.GetGuid(1),
                    Payload = reader.GetFieldValue<byte[]>(2),
                    AttemptCount = reader.GetInt32(3)
                });
            }

            // Drain remaining results so a COMMIT error (rare) surfaces here instead of being lost.
            while (await reader.NextResultAsync(ct)) { }
        }

        return claimed;
    }

    /// <summary>
    /// Writes the results of messages already handled when the listener stopped, using a fresh token because the
    /// listening token is cancelled. Messages claimed but not yet handled stay InProgress and are redelivered
    /// once their lease expires.
    /// </summary>
    private async Task FinalizeOnShutdownAsync(List<MessageCompletion> unfinalized)
    {
        if (unfinalized.Count == 0)
            return;

        try
        {
            using var cts = new CancellationTokenSource(ShutdownFinalizeTimeout);
            await using var conn = await OpenConnectionAsync(cts.Token);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = SqlServerQueueSql.Finalize(_settings.TableName);
            AddItemsParameter(cmd, unfinalized);
            await cmd.ExecuteNonQueryAsync(cts.Token);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not record results of {Count} message(s) on endpoint '{Name}' during shutdown; they will be redelivered after their lease expires",
                unfinalized.Count, _name);
        }
    }

    private static void AddItemsParameter(SqlCommand cmd, List<MessageCompletion> completions)
    {
        var items = completions.Count == 0
            ? (object)DBNull.Value
            : JsonSerializer.Serialize(
                completions.Select(static c => new FinalizeItem(c.QueueId, c.AttemptCount, (int)c.Status, c.Error)),
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

    private int GetLeaseTimeoutSeconds()
        => (int)Math.Max(1, Math.Ceiling(_settings.LeaseTimeout.TotalSeconds));

    private sealed record BatchItem(int I, byte[] P);

    private sealed record FinalizeItem(long Id, int Attempt, int Status, string? Error);

    private enum MessageStatus
    {
        Pending = SqlServerQueueSql.Pending,
        InProgress = SqlServerQueueSql.InProgress,
        Completed = SqlServerQueueSql.Completed,
        Failed = SqlServerQueueSql.Failed
    }

    private sealed class ClaimedMessage
    {
        public long QueueId     { get; set; }
        public Guid MessageId   { get; set; }
        public byte[] Payload   { get; set; } = [];
        public int AttemptCount { get; set; }
    }

    private sealed class MessageCompletion
    {
        public long QueueId { get; private init; }
        public int AttemptCount { get; private init; }
        public MessageStatus Status { get; private init; }
        public string? Error { get; private init; }

        public static MessageCompletion Completed(ClaimedMessage message)
            => new() { QueueId = message.QueueId, AttemptCount = message.AttemptCount, Status = MessageStatus.Completed };

        public static MessageCompletion FromFailure(ClaimedMessage message, MessageStatus status, string? error)
            => new() { QueueId = message.QueueId, AttemptCount = message.AttemptCount, Status = status, Error = error };
    }
}
