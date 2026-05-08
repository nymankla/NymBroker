using System.Runtime.CompilerServices;
using System.Text;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Sql;

public sealed class SqliteEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private const string ReadAsyncLeaseReturnedMessage = "Message lease returned by ReadAsync without acknowledgement.";

    private readonly SqliteSettings _settings;
    private readonly ILogger<SqliteEndPoint> _logger;
    // Serializes all DB operations — ensures single-connection SQLite is never accessed concurrently.
    private readonly SemaphoreSlim _dbLock = new(1, 1);
    private SqliteConnection? _connection;
    private CancellationTokenSource? _listeningCts;

    public string Name { get; }

    public SqliteEndPoint(string name, SqliteSettings settings, ILogger<SqliteEndPoint> logger)
    {
        Name = name;
        _settings = settings;
        _logger = logger;
    }

    // ── IEndPointEventDriven ────────────────────────────────────────────────

    public Task StartListeningAsync(Func<string, CancellationToken, Task> handler, CancellationToken ct)
    {
        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    var processed = 0;
                    try
                    {
                        var messages = await ClaimMessagesAsync(token);
                        foreach (var message in messages)
                        {
                            try
                            {
                                await handler(message.Payload, token);
                                await FinalizeClaimedMessageAsync(message, succeeded: true, error: null, token);
                            }
                            catch (Exception ex) when (ex is not OperationCanceledException)
                            {
                                await FinalizeClaimedMessageAsync(message, succeeded: false, error: ex.Message, token);
                                _logger.LogError(ex, "Unhandled error dispatching message on endpoint '{Name}'", Name);
                            }
                            processed++;
                        }
                    }
                    catch (OperationCanceledException) { return; }
                    catch (Exception ex) { _logger.LogError(ex, "Poll error on endpoint '{Name}'", Name); }

                    // Back off when idle; poll immediately after a full batch when PollInterval is zero.
                    var delayMs = processed == 0
                        ? (int)Math.Max(1, _settings.PollInterval.TotalMilliseconds)
                        : (int)_settings.PollInterval.TotalMilliseconds;
                    if (delayMs > 0)
                    {
                        try { await Task.Delay(delayMs, token); }
                        catch (OperationCanceledException) { return; }
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", Name);
            }
        }, CancellationToken.None);

        return Task.CompletedTask;
    }

    public Task StopListeningAsync()
    {
        _listeningCts?.Cancel();
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<string> ReadAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var messages = await ClaimMessagesAsync(ct);
        foreach (var message in messages)
        {
            if (ct.IsCancellationRequested) yield break;

            var completed = false;
            try
            {
                yield return message.Payload;
                completed = true;
            }
            finally
            {
                await FinalizeClaimedMessageAsync(
                    message,
                    succeeded: completed,
                    error: completed ? null : ReadAsyncLeaseReturnedMessage,
                    CancellationToken.None);
            }
        }
    }

    // ── IEndPoint ───────────────────────────────────────────────────────────

    public async Task PostAsync(Stream message, CancellationToken ct = default)
    {
        using var reader = new StreamReader(message, Encoding.UTF8, leaveOpen: true);
        var payload = await reader.ReadToEndAsync(ct);

        await _dbLock.WaitAsync(ct);
        try
        {
            var conn = await EnsureConnectionAsync(ct);
            await conn.ExecuteAsync(
                $"INSERT INTO {_settings.TableName} (MessageId, Status, CreatedAtUtc, AttemptCount, Payload) VALUES (@MessageId, @Status, unixepoch(), 0, @Payload)",
                new { MessageId = Guid.NewGuid().ToString(), Status = (int)MessageStatus.Pending, Payload = payload });
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public IHealthCheckResult HealthCheck()
    {
        try
        {
            if (_connection?.State == System.Data.ConnectionState.Open)
                return HealthCheckResult.Healthy();

            using var probe = new SqliteConnection(_settings.ConnectionString);
            probe.Open();
            probe.ExecuteScalar<int>("SELECT 1");
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SQLite endpoint '{Name}' health check failed", Name);
            return HealthCheckResult.Unhealthy(ex.Message);
        }
    }

    // ── IAsyncDisposable ────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        _listeningCts?.Cancel();
        _listeningCts?.Dispose();
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
        _dbLock.Dispose();
    }

    // ── Private ─────────────────────────────────────────────────────────────

    // Must be called while holding _dbLock.
    private async Task<SqliteConnection> EnsureConnectionAsync(CancellationToken ct)
    {
        if (_connection is not null) return _connection;

        var conn = new SqliteConnection(_settings.ConnectionString);
        await conn.OpenAsync(ct);

        if (_settings.AutoCreateTable)
            await EnsureSchemaAsync(conn, ct);

        _connection = conn;
        return _connection;
    }

    private async Task EnsureSchemaAsync(SqliteConnection conn, CancellationToken ct)
    {
        var tableExists = await conn.ExecuteScalarAsync<long>(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = @TableName",
            new { TableName = _settings.TableName }) > 0;

        if (!tableExists)
        {
            await CreateRobustSchemaAsync(conn);
            return;
        }

        var columns = (await conn.QueryAsync<TableColumnInfo>($"PRAGMA table_info({_settings.TableName})")).ToList();
        if (columns.Any(c => string.Equals(c.Name, "QueueId", StringComparison.OrdinalIgnoreCase))
            && columns.Any(c => string.Equals(c.Name, "MessageId", StringComparison.OrdinalIgnoreCase)))
        {
            await EnsureIndexesAsync(conn);
            return;
        }

        await MigrateLegacySchemaAsync(conn, ct);
    }

    private async Task CreateRobustSchemaAsync(SqliteConnection conn)
    {
        await conn.ExecuteAsync($"""
            CREATE TABLE IF NOT EXISTS {_settings.TableName} (
                QueueId         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                MessageId       TEXT    NOT NULL UNIQUE,
                Status          INTEGER NOT NULL DEFAULT 0 CHECK (Status IN (0, 1, 2, 3)),
                CreatedAtUtc    INTEGER NOT NULL DEFAULT (unixepoch()),
                LockedUntilUtc  INTEGER NULL,
                CompletedAtUtc  INTEGER NULL,
                FailedAtUtc     INTEGER NULL,
                AttemptCount    INTEGER NOT NULL DEFAULT 0,
                LastError       TEXT NULL,
                Payload         TEXT NOT NULL
            )
            """);
        await EnsureIndexesAsync(conn);
    }

    private async Task EnsureIndexesAsync(SqliteConnection conn)
    {
        await conn.ExecuteAsync($"""
            CREATE INDEX IF NOT EXISTS IX_{_settings.TableName}_Status_CreatedAt
                ON {_settings.TableName}(Status, CreatedAtUtc, QueueId)
            """);
        await conn.ExecuteAsync($"""
            CREATE INDEX IF NOT EXISTS IX_{_settings.TableName}_LockedUntil
                ON {_settings.TableName}(Status, LockedUntilUtc)
            """);
    }

    private async Task MigrateLegacySchemaAsync(SqliteConnection conn, CancellationToken ct)
    {
        var backupTableName = $"{_settings.TableName}_Legacy_{DateTime.UtcNow:yyyyMMddHHmmss}";
        using var tx = await conn.BeginTransactionAsync(ct);

        await conn.ExecuteAsync($"ALTER TABLE {_settings.TableName} RENAME TO {backupTableName}", transaction: tx);
        await CreateRobustSchemaAsync(conn);
        await conn.ExecuteAsync($"""
            INSERT INTO {_settings.TableName} (
                MessageId,
                Status,
                CreatedAtUtc,
                LockedUntilUtc,
                CompletedAtUtc,
                FailedAtUtc,
                AttemptCount,
                LastError,
                Payload)
            SELECT
                Id,
                CASE
                    WHEN Status = 'Processed' THEN @CompletedStatus
                    ELSE @PendingStatus
                END,
                COALESCE(unixepoch(CreatedAt), unixepoch()),
                NULL,
                CASE
                    WHEN ProcessedAt IS NOT NULL THEN unixepoch(ProcessedAt)
                    ELSE NULL
                END,
                NULL,
                CASE
                    WHEN Status = 'Processed' THEN 1
                    ELSE 0
                END,
                NULL,
                Payload
            FROM {backupTableName}
            """,
            new
            {
                PendingStatus = (int)MessageStatus.Pending,
                CompletedStatus = (int)MessageStatus.Completed
            }, tx);

        await tx.CommitAsync(ct);
    }

    private async Task<List<ClaimedMessage>> ClaimMessagesAsync(CancellationToken ct)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            var conn = await EnsureConnectionAsync(ct);
            using var tx = await conn.BeginTransactionAsync(ct);

            var rows = (await conn.QueryAsync<QueuedMessageRow>($"""
                SELECT QueueId, MessageId, Payload, AttemptCount
                FROM {_settings.TableName}
                WHERE Status = @PendingStatus
                   OR (Status = @InProgressStatus AND LockedUntilUtc IS NOT NULL AND LockedUntilUtc <= unixepoch())
                ORDER BY CreatedAtUtc, QueueId
                LIMIT @BatchSize
                """,
                new
                {
                    PendingStatus = (int)MessageStatus.Pending,
                    InProgressStatus = (int)MessageStatus.InProgress,
                    _settings.BatchSize
                }, tx)).ToList();

            var claimed = new List<ClaimedMessage>(rows.Count);
            foreach (var row in rows)
            {
                var updated = await conn.ExecuteAsync($"""
                    UPDATE {_settings.TableName}
                    SET Status = @InProgressStatus,
                        LockedUntilUtc = unixepoch() + @LeaseTimeoutSeconds,
                        AttemptCount = AttemptCount + 1,
                        LastError = NULL,
                        FailedAtUtc = NULL
                    WHERE QueueId = @QueueId
                      AND (Status = @PendingStatus
                        OR (Status = @InProgressStatus AND LockedUntilUtc IS NOT NULL AND LockedUntilUtc <= unixepoch()))
                    """,
                    new
                    {
                        row.QueueId,
                        PendingStatus = (int)MessageStatus.Pending,
                        InProgressStatus = (int)MessageStatus.InProgress,
                        LeaseTimeoutSeconds = GetLeaseTimeoutSeconds()
                    }, tx);

                if (updated > 0)
                {
                    claimed.Add(new ClaimedMessage
                    {
                        QueueId = row.QueueId,
                        MessageId = row.MessageId,
                        Payload = row.Payload,
                        AttemptCount = row.AttemptCount + 1
                    });
                }
            }

            await tx.CommitAsync(ct);
            return claimed;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    private async Task FinalizeClaimedMessageAsync(ClaimedMessage message, bool succeeded, string? error, CancellationToken ct)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            var conn = await EnsureConnectionAsync(ct);
            if (succeeded)
            {
                await conn.ExecuteAsync($"""
                    UPDATE {_settings.TableName}
                    SET Status = @CompletedStatus,
                        LockedUntilUtc = NULL,
                        CompletedAtUtc = unixepoch(),
                        FailedAtUtc = NULL,
                        LastError = NULL
                    WHERE QueueId = @QueueId
                      AND Status = @InProgressStatus
                    """,
                    new
                    {
                        message.QueueId,
                        CompletedStatus = (int)MessageStatus.Completed,
                        InProgressStatus = (int)MessageStatus.InProgress
                    });
                return;
            }

            if (message.AttemptCount >= _settings.MaxRetryCount)
            {
                await conn.ExecuteAsync($"""
                    UPDATE {_settings.TableName}
                    SET Status = @FailedStatus,
                        LockedUntilUtc = NULL,
                        FailedAtUtc = unixepoch(),
                        LastError = @Error,
                        CompletedAtUtc = NULL
                    WHERE QueueId = @QueueId
                      AND Status = @InProgressStatus
                    """,
                    new
                    {
                        message.QueueId,
                        FailedStatus = (int)MessageStatus.Failed,
                        InProgressStatus = (int)MessageStatus.InProgress,
                        Error = error
                    });
                return;
            }

            await conn.ExecuteAsync($"""
                UPDATE {_settings.TableName}
                SET Status = @PendingStatus,
                    LockedUntilUtc = NULL,
                    LastError = @Error,
                    FailedAtUtc = NULL,
                    CompletedAtUtc = NULL
                WHERE QueueId = @QueueId
                  AND Status = @InProgressStatus
                """,
                new
                {
                    message.QueueId,
                    PendingStatus = (int)MessageStatus.Pending,
                    InProgressStatus = (int)MessageStatus.InProgress,
                    Error = error
                });
        }
        finally
        {
            _dbLock.Release();
        }
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
        public long QueueId       { get; set; }
        public string MessageId   { get; set; } = "";
        public string Payload     { get; set; } = "";
        public int AttemptCount   { get; set; }
    }

    private sealed class QueuedMessageRow
    {
        public long QueueId       { get; set; }
        public string MessageId   { get; set; } = "";
        public string Payload     { get; set; } = "";
        public int AttemptCount   { get; set; }
    }

    private sealed class TableColumnInfo
    {
        public string Name { get; set; } = "";
    }
}
