using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Postgres;

/// <summary>
/// PostgreSQL queue endpoint. Rows are claimed with a lease (<c>FOR UPDATE SKIP LOCKED</c>, so several instances
/// can poll one table), dispatched, then marked Completed, returned to Pending, or marked Failed after
/// <see cref="PostgresSettings.MaxRetryCount"/>.
/// <para>
/// While messages are waiting, batches are claimed back to back; a batch's results are written in the same
/// round trip and transaction that claims the next batch. When the queue is empty the loop waits for a
/// <c>NOTIFY</c> (if <see cref="PostgresSettings.UseNotifications"/>) or <see cref="PostgresSettings.PollInterval"/>.
/// </para>
/// </summary>
public sealed class PostgresEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private static readonly TimeSpan ShutdownFinalizeTimeout = TimeSpan.FromSeconds(10);

    private readonly PostgresSettings _settings;
    private readonly ILogger<PostgresEndPoint> _logger;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private readonly string _name;

    private NpgsqlDataSource? _dataSource;
    private bool _schemaEnsured;
    private CancellationTokenSource? _listeningCts;
    private Task? _loop;

    public EndpointMode Mode { get; }

    public PostgresEndPoint(string name, PostgresSettings settings, ILogger<PostgresEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
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

    /// <summary>Stops polling and waits for the loop to finish, so no handler runs after this returns.</summary>
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
        cmd.CommandText = PostgresQueueSql.InsertMessage(_settings.TableName, _settings.UseNotifications);
        cmd.Parameters.AddWithValue("messageId", Guid.NewGuid());
        cmd.Parameters.Add(new NpgsqlParameter<byte[]>("payload", NpgsqlDbType.Bytea) { TypedValue = message });
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
            _logger.LogError(ex, "PostgreSQL endpoint '{Name}' health check failed", _name);
            return HealthCheckResult.Unhealthy(ex.Message);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopListeningAsync();
        _listeningCts?.Dispose();
        _listeningCts = null;
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
            _dataSource = null;
        }
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
        NpgsqlConnection? notificationConnection = null;
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
                    // The batch rolled back, so 'unfinalized' is retried on the next cycle. If the leases expire
                    // first, the attempt_count guard turns that late finalize into a no-op.
                    _logger.LogError(ex, "Poll error on endpoint '{Name}'", _name);
                }

                // Drain without waiting while there is work; wait for NOTIFY / PollInterval only when idle.
                if (claimedCount == 0)
                    notificationConnection = await WaitForNextCycleAsync(notificationConnection, token);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", _name);
        }
        finally
        {
            if (notificationConnection is not null)
                await notificationConnection.DisposeAsync();
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

    /// <summary>
    /// Finalizes the previous batch and claims the next one in a single <see cref="NpgsqlBatch"/>: one round trip,
    /// executed by PostgreSQL as one implicit transaction (one commit / WAL flush per batch).
    /// </summary>
    private async Task<List<ClaimedMessage>> FinalizeAndClaimAsync(List<MessageCompletion> unfinalized, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var batch = conn.CreateBatch();

        if (unfinalized.Count > 0)
            batch.BatchCommands.Add(CreateFinalizeCommand(unfinalized));

        var claim = new NpgsqlBatchCommand(PostgresQueueSql.ClaimMessages(_settings.TableName));
        claim.Parameters.Add(new NpgsqlParameter<int>("batchSize", _settings.BatchSize));
        claim.Parameters.Add(new NpgsqlParameter<int>("leaseTimeout", GetLeaseTimeoutSeconds()));
        batch.BatchCommands.Add(claim);

        var claimed = new List<ClaimedMessage>();
        await using var reader = await batch.ExecuteReaderAsync(ct);
        do
        {
            // The finalize UPDATE returns no rows; the claim's RETURNING rows are the only result set with fields.
            if (reader.FieldCount == 0)
                continue;

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
        }
        while (await reader.NextResultAsync(ct));

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
            await using var batch = conn.CreateBatch();
            batch.BatchCommands.Add(CreateFinalizeCommand(unfinalized));
            await batch.ExecuteNonQueryAsync(cts.Token);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not record results of {Count} message(s) on endpoint '{Name}' during shutdown; they will be redelivered after their lease expires",
                unfinalized.Count, _name);
        }
    }

    private NpgsqlBatchCommand CreateFinalizeCommand(List<MessageCompletion> completions)
    {
        var cmd = new NpgsqlBatchCommand(PostgresQueueSql.FinalizeMessages(_settings.TableName));
        cmd.Parameters.Add(new NpgsqlParameter<long[]>("queueIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
        {
            TypedValue = completions.Select(static c => c.QueueId).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter<int[]>("attempts", NpgsqlDbType.Array | NpgsqlDbType.Integer)
        {
            TypedValue = completions.Select(static c => c.AttemptCount).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter<int[]>("statuses", NpgsqlDbType.Array | NpgsqlDbType.Integer)
        {
            TypedValue = completions.Select(static c => (int)c.Status).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter<string?[]>("errors", NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            TypedValue = completions.Select(static c => c.Error).ToArray()
        });
        return cmd;
    }

    /// <summary>
    /// Called only after an empty poll. Waits for a NOTIFY (or <see cref="PostgresSettings.PollInterval"/>, whichever
    /// comes first). If the LISTEN connection cannot be opened or fails, falls back to a plain delay and retries
    /// LISTEN on the next idle cycle instead of terminating the loop.
    /// </summary>
    private async Task<NpgsqlConnection?> WaitForNextCycleAsync(NpgsqlConnection? notificationConnection, CancellationToken token)
    {
        var delay = TimeSpan.FromMilliseconds(Math.Max(1, _settings.PollInterval.TotalMilliseconds));
        if (!_settings.UseNotifications)
        {
            await Task.Delay(delay, token);
            return notificationConnection;
        }

        try
        {
            notificationConnection ??= await OpenNotificationConnectionAsync(token);
            using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            waitCts.CancelAfter(delay);
            await notificationConnection.WaitAsync(waitCts.Token);
            return notificationConnection;
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            return notificationConnection;   // poll interval elapsed without a notification
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Notification wait failed on endpoint '{Name}', falling back to timer-based polling", _name);
            if (notificationConnection is not null)
                await notificationConnection.DisposeAsync();
            await Task.Delay(delay, token);
            return null;
        }
    }

    private async Task<NpgsqlConnection> OpenNotificationConnectionAsync(CancellationToken ct)
    {
        var conn = await OpenConnectionAsync(ct);
        try
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = PostgresQueueSql.Listen(_settings.TableName);
            await cmd.ExecuteNonQueryAsync(ct);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var dataSource = await EnsureDataSourceAsync(ct);
        var conn = await dataSource.OpenConnectionAsync(ct);
        try
        {
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

    private async Task<NpgsqlDataSource> EnsureDataSourceAsync(CancellationToken ct)
    {
        if (_dataSource is not null) return _dataSource;

        await _schemaLock.WaitAsync(ct);
        try
        {
            if (_dataSource is not null) return _dataSource;

            var csb = new NpgsqlConnectionStringBuilder(_settings.ConnectionString)
            {
                MaxAutoPrepare = 32,
                AutoPrepareMinUsages = 2
            };
            var builder = new NpgsqlDataSourceBuilder(csb.ConnectionString);
            _dataSource = builder.Build();
            return _dataSource;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    private async Task EnsureSchemaAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        if (_schemaEnsured) return;

        await _schemaLock.WaitAsync(ct);
        try
        {
            if (_schemaEnsured) return;

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = PostgresQueueSql.CreateSchema(_settings.TableName);
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

    private enum MessageStatus
    {
        Pending = PostgresQueueSql.Pending,
        InProgress = PostgresQueueSql.InProgress,
        Completed = PostgresQueueSql.Completed,
        Failed = PostgresQueueSql.Failed
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
