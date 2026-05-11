using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Postgres;

public sealed class PostgresEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private readonly PostgresSettings _settings;
    private readonly ILogger<PostgresEndPoint> _logger;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private readonly string _name;

    private NpgsqlDataSource? _dataSource;
    private bool _schemaEnsured;
    private CancellationTokenSource? _listeningCts;

    public EndpointMode Mode { get; }

    public PostgresEndPoint(string name, PostgresSettings settings, ILogger<PostgresEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        Mode = mode;
        _settings = settings;
        _logger = logger;
    }

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct)
    {
        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;

        _ = Task.Run(() => RunListenerLoopAsync(handler, token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public Task StopListeningAsync()
    {
        _listeningCts?.Cancel();
        return Task.CompletedTask;
    }

    public async Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresQueueSql.InsertMessage(_settings.TableName, _settings.UseNotifications);
        cmd.Parameters.AddWithValue("messageId", Guid.NewGuid());
        cmd.Parameters.AddWithValue("status", (int)MessageStatus.Pending);
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
        _listeningCts?.Cancel();
        _listeningCts?.Dispose();
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

    private async Task RunListenerLoopAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken token)
    {
        NpgsqlConnection? notificationConnection = null;
        try
        {
            if (_settings.UseNotifications)
                notificationConnection = await OpenNotificationConnectionAsync(token);

            while (!token.IsCancellationRequested)
            {
                var processed = 0;
                try
                {
                    var messages = await ClaimMessagesAsync(token);
                    processed = messages.Count;
                    if (processed > 0)
                        await ProcessClaimedMessagesAsync(messages, handler, token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Poll error on endpoint '{Name}'", _name);
                }

                notificationConnection = await WaitForNextCycleAsync(notificationConnection, processed, token);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", _name);
        }
        finally
        {
            if (notificationConnection is not null)
                await notificationConnection.DisposeAsync();
        }
    }

    private async Task ProcessClaimedMessagesAsync(List<ClaimedMessage> messages, Func<byte[], CancellationToken, Task> handler, CancellationToken ct)
    {
        var completions = new List<MessageCompletion>(messages.Count);
        foreach (var message in messages)
        {
            try
            {
                await handler(message.Payload, ct);
                completions.Add(MessageCompletion.Completed(message));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                completions.Add(MessageCompletion.FromFailure(
                    message,
                    message.AttemptCount >= _settings.MaxRetryCount ? MessageStatus.Failed : MessageStatus.Pending,
                    ex.Message));
                _logger.LogError(ex, "Unhandled error dispatching message on endpoint '{Name}'", _name);
            }
        }

        await FinalizeClaimedMessagesAsync(completions, ct);
    }

    private async Task FinalizeClaimedMessagesAsync(List<MessageCompletion> completions, CancellationToken ct)
    {
        if (completions.Count == 0)
            return;

        await using var conn = await OpenConnectionAsync(ct);

        var completedIds = completions
            .Where(static completion => completion.Status == MessageStatus.Completed)
            .Select(static completion => completion.QueueId)
            .ToArray();
        if (completedIds.Length > 0)
            await ExecuteCompletedUpdateAsync(conn, completedIds, ct);

        var retry = completions.Where(static completion => completion.Status == MessageStatus.Pending).ToList();
        if (retry.Count > 0)
            await ExecuteErroredUpdateAsync(conn, retry, MessageStatus.Pending, ct);

        var failed = completions.Where(static completion => completion.Status == MessageStatus.Failed).ToList();
        if (failed.Count > 0)
            await ExecuteErroredUpdateAsync(conn, failed, MessageStatus.Failed, ct);
    }

    private async Task ExecuteCompletedUpdateAsync(NpgsqlConnection conn, long[] queueIds, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresQueueSql.FinalizeCompleted(_settings.TableName);
        cmd.Parameters.Add(new NpgsqlParameter<long[]>("queueIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { TypedValue = queueIds });
        cmd.Parameters.AddWithValue("completedStatus", (int)MessageStatus.Completed);
        cmd.Parameters.AddWithValue("inProgressStatus", (int)MessageStatus.InProgress);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task ExecuteErroredUpdateAsync(NpgsqlConnection conn, List<MessageCompletion> completions, MessageStatus status, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresQueueSql.FinalizeWithErrors(_settings.TableName);
        cmd.Parameters.Add(new NpgsqlParameter<long[]>("queueIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
        {
            TypedValue = completions.Select(static completion => completion.QueueId).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter<string?[]>("errors", NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            TypedValue = completions.Select(static completion => completion.Error).ToArray()
        });
        cmd.Parameters.AddWithValue("status", (int)status);
        cmd.Parameters.AddWithValue("failedStatus", (int)MessageStatus.Failed);
        cmd.Parameters.AddWithValue("inProgressStatus", (int)MessageStatus.InProgress);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task<NpgsqlConnection?> WaitForNextCycleAsync(NpgsqlConnection? notificationConnection, int processed, CancellationToken token)
    {
        var delayMs = processed == 0
            ? (int)Math.Max(1, _settings.PollInterval.TotalMilliseconds)
            : (int)_settings.PollInterval.TotalMilliseconds;
        if (delayMs <= 0)
            return notificationConnection;

        if (!_settings.UseNotifications)
        {
            await Task.Delay(delayMs, token);
            return notificationConnection;
        }

        notificationConnection ??= await OpenNotificationConnectionAsync(token);
        using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        waitCts.CancelAfter(delayMs);
        try
        {
            await notificationConnection.WaitAsync(waitCts.Token);
            return notificationConnection;
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            return notificationConnection;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Notification wait failed on endpoint '{Name}', falling back to timer-based polling", _name);
            await notificationConnection.DisposeAsync();
            return null;
        }
    }

    private async Task<NpgsqlConnection> OpenNotificationConnectionAsync(CancellationToken ct)
    {
        var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresQueueSql.Listen(_settings.TableName);
        await cmd.ExecuteNonQueryAsync(ct);
        return conn;
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var dataSource = await EnsureDataSourceAsync(ct);
        var conn = await dataSource.OpenConnectionAsync(ct);
        if (_settings.AutoCreateTable)
            await EnsureSchemaAsync(conn, ct);
        return conn;
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

    private async Task<List<ClaimedMessage>> ClaimMessagesAsync(CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = PostgresQueueSql.ClaimMessages(_settings.TableName);
        cmd.Parameters.AddWithValue("pendingStatus", (int)MessageStatus.Pending);
        cmd.Parameters.AddWithValue("inProgressStatus", (int)MessageStatus.InProgress);
        cmd.Parameters.AddWithValue("batchSize", _settings.BatchSize);
        cmd.Parameters.AddWithValue("leaseTimeout", GetLeaseTimeoutSeconds());

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
        }

        await tx.CommitAsync(ct);
        return claimed;
    }

    private int GetLeaseTimeoutSeconds()
        => (int)Math.Max(1, Math.Ceiling(_settings.LeaseTimeout.TotalSeconds));

    private enum MessageStatus
    {
        Pending = 0,
        InProgress = 1,
        Completed = 2,
        Failed = 3
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
        public MessageStatus Status { get; private init; }
        public string? Error { get; private init; }

        public static MessageCompletion Completed(ClaimedMessage message)
            => new() { QueueId = message.QueueId, Status = MessageStatus.Completed };

        public static MessageCompletion FromFailure(ClaimedMessage message, MessageStatus status, string? error)
            => new() { QueueId = message.QueueId, Status = status, Error = error };
    }
}
