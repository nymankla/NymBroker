using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Logging;
using Npgsql;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Postgres;

public sealed class PostgresEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private const string ReadAsyncLeaseReturnedMessage = "Message lease returned by ReadAsync without acknowledgement.";

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
                                await handler(Encoding.UTF8.GetBytes(message.Payload), token);
                                await FinalizeClaimedMessageAsync(message, succeeded: true, error: null, token);
                            }
                            catch (Exception ex) when (ex is not OperationCanceledException)
                            {
                                await FinalizeClaimedMessageAsync(message, succeeded: false, error: ex.Message, token);
                                _logger.LogError(ex, "Unhandled error dispatching message on endpoint '{Name}'", _name);
                            }
                            processed++;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Poll error on endpoint '{Name}'", _name);
                    }

                    var delayMs = processed == 0
                        ? (int)Math.Max(1, _settings.PollInterval.TotalMilliseconds)
                        : (int)_settings.PollInterval.TotalMilliseconds;
                    if (delayMs > 0)
                    {
                        try
                        {
                            await Task.Delay(delayMs, token);
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", _name);
            }
        }, CancellationToken.None);

        return Task.CompletedTask;
    }

    public Task StopListeningAsync()
    {
        _listeningCts?.Cancel();
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<string> ReadAsync([EnumeratorCancellation] CancellationToken ct = default)
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

    public async Task PostAsync(Stream message, CancellationToken ct = default)
    {
        using var reader = new StreamReader(message, Encoding.UTF8, leaveOpen: true);
        var payload = await reader.ReadToEndAsync(ct);

        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            INSERT INTO {QuoteIdentifier(_settings.TableName)}
                (message_id, status, created_at_utc, attempt_count, payload)
            VALUES
                (@messageId, @status, NOW(), 0, @payload)
            """;
        cmd.Parameters.AddWithValue("messageId", Guid.NewGuid());
        cmd.Parameters.AddWithValue("status", (int)MessageStatus.Pending);
        cmd.Parameters.AddWithValue("payload", payload);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public IHealthCheckResult HealthCheck()
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var result = HealthCheckAsync(cts.Token).GetAwaiter().GetResult();
            return result;
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
            _dataSource = NpgsqlDataSource.Create(_settings.ConnectionString);
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
            cmd.CommandText = $"""
                CREATE TABLE IF NOT EXISTS {QuoteIdentifier(_settings.TableName)} (
                    queue_id         BIGSERIAL PRIMARY KEY,
                    message_id       UUID        NOT NULL UNIQUE,
                    status           INTEGER     NOT NULL DEFAULT 0 CHECK (status IN (0, 1, 2, 3)),
                    created_at_utc   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    locked_until_utc TIMESTAMPTZ NULL,
                    completed_at_utc TIMESTAMPTZ NULL,
                    failed_at_utc    TIMESTAMPTZ NULL,
                    attempt_count    INTEGER     NOT NULL DEFAULT 0,
                    last_error       TEXT        NULL,
                    payload          TEXT        NOT NULL
                );
                CREATE INDEX IF NOT EXISTS {QuoteIdentifier($"ix_{_settings.TableName}_status_created")}
                    ON {QuoteIdentifier(_settings.TableName)}(status, created_at_utc, queue_id);
                CREATE INDEX IF NOT EXISTS {QuoteIdentifier($"ix_{_settings.TableName}_status_locked_until")}
                    ON {QuoteIdentifier(_settings.TableName)}(status, locked_until_utc);
                """;
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
        cmd.CommandText = $"""
            WITH claimed AS (
                SELECT queue_id
                FROM {QuoteIdentifier(_settings.TableName)}
                WHERE status = @pendingStatus
                   OR (status = @inProgressStatus AND locked_until_utc IS NOT NULL AND locked_until_utc <= NOW())
                ORDER BY created_at_utc, queue_id
                FOR UPDATE SKIP LOCKED
                LIMIT @batchSize
            )
            UPDATE {QuoteIdentifier(_settings.TableName)} AS m
            SET status = @inProgressStatus,
                locked_until_utc = NOW() + (@leaseTimeout * INTERVAL '1 second'),
                attempt_count = m.attempt_count + 1,
                last_error = NULL,
                failed_at_utc = NULL
            FROM claimed
            WHERE m.queue_id = claimed.queue_id
            RETURNING m.queue_id, m.message_id, m.payload, m.attempt_count;
            """;
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
                    Payload = reader.GetString(2),
                    AttemptCount = reader.GetInt32(3)
                });
            }
        }

        await tx.CommitAsync(ct);
        return claimed;
    }

    private async Task FinalizeClaimedMessageAsync(ClaimedMessage message, bool succeeded, string? error, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();

        if (succeeded)
        {
            cmd.CommandText = $"""
                UPDATE {QuoteIdentifier(_settings.TableName)}
                SET status = @completedStatus,
                    locked_until_utc = NULL,
                    completed_at_utc = NOW(),
                    failed_at_utc = NULL,
                    last_error = NULL
                WHERE queue_id = @queueId
                  AND status = @inProgressStatus
                """;
            cmd.Parameters.AddWithValue("queueId", message.QueueId);
            cmd.Parameters.AddWithValue("completedStatus", (int)MessageStatus.Completed);
            cmd.Parameters.AddWithValue("inProgressStatus", (int)MessageStatus.InProgress);
            await cmd.ExecuteNonQueryAsync(ct);
            return;
        }

        if (message.AttemptCount >= _settings.MaxRetryCount)
        {
            cmd.CommandText = $"""
                UPDATE {QuoteIdentifier(_settings.TableName)}
                SET status = @failedStatus,
                    locked_until_utc = NULL,
                    failed_at_utc = NOW(),
                    last_error = @error,
                    completed_at_utc = NULL
                WHERE queue_id = @queueId
                  AND status = @inProgressStatus
                """;
            cmd.Parameters.AddWithValue("queueId", message.QueueId);
            cmd.Parameters.AddWithValue("failedStatus", (int)MessageStatus.Failed);
            cmd.Parameters.AddWithValue("inProgressStatus", (int)MessageStatus.InProgress);
            cmd.Parameters.AddWithValue("error", (object?)error ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);
            return;
        }

        cmd.CommandText = $"""
            UPDATE {QuoteIdentifier(_settings.TableName)}
            SET status = @pendingStatus,
                locked_until_utc = NULL,
                last_error = @error,
                failed_at_utc = NULL,
                completed_at_utc = NULL
            WHERE queue_id = @queueId
              AND status = @inProgressStatus
            """;
        cmd.Parameters.AddWithValue("queueId", message.QueueId);
        cmd.Parameters.AddWithValue("pendingStatus", (int)MessageStatus.Pending);
        cmd.Parameters.AddWithValue("inProgressStatus", (int)MessageStatus.InProgress);
        cmd.Parameters.AddWithValue("error", (object?)error ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private int GetLeaseTimeoutSeconds()
        => (int)Math.Max(1, Math.Ceiling(_settings.LeaseTimeout.TotalSeconds));

    internal static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("PostgreSQL identifier cannot be empty.");

        return string.Join('.', identifier.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(part => $"\"{part.Replace("\"", "\"\"")}\""));
    }

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
        public string Payload   { get; set; } = string.Empty;
        public int AttemptCount { get; set; }
    }
}
