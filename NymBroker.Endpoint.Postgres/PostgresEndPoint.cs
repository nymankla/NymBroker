using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Endpoint.Postgres;

/// <summary>
/// PostgreSQL queue endpoint. Rows are claimed with a lease (<c>FOR UPDATE SKIP LOCKED</c>, so several instances
/// can poll one table), dispatched, then marked Completed, returned to Pending, or marked Failed after
/// <see cref="PostgresSettings.MaxRetryCount"/>. The loop itself is the shared <see cref="LeasedQueueListener"/>.
/// <para>
/// While messages are waiting, batches are claimed back to back; a batch's results are written in the same
/// round trip and transaction that claims the next batch. When the queue is empty the loop waits for a
/// <c>NOTIFY</c> (if <see cref="PostgresSettings.UseNotifications"/>) or <see cref="PostgresSettings.PollInterval"/>.
/// </para>
/// </summary>
public sealed class PostgresEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private readonly PostgresSettings _settings;
    private readonly ILogger<PostgresEndPoint> _logger;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private readonly string _name;
    private readonly Listener _listener;

    private NpgsqlDataSource? _dataSource;
    private bool _schemaEnsured;

    public EndpointMode Mode { get; }

    public PostgresEndPoint(string name, PostgresSettings settings, ILogger<PostgresEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
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
        cmd.CommandText = PostgresQueueSql.InsertMessage(_settings.TableName, _settings.UseNotifications);
        cmd.Parameters.AddWithValue("messageId", Guid.NewGuid());
        cmd.Parameters.Add(new NpgsqlParameter<byte[]>("payload", NpgsqlDbType.Bytea) { TypedValue = message });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>All messages in one INSERT (one transaction, one NOTIFY); atomic, order preserved.</summary>
    public async Task PostBatchAsync(IReadOnlyList<byte[]> messages, CancellationToken ct = default)
    {
        if (messages.Count == 0) return;

        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresQueueSql.InsertMessages(_settings.TableName, _settings.UseNotifications);
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]>("messageIds", NpgsqlDbType.Array | NpgsqlDbType.Uuid)
        {
            TypedValue = messages.Select(static _ => Guid.NewGuid()).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter<byte[][]>("payloads", NpgsqlDbType.Array | NpgsqlDbType.Bytea)
        {
            TypedValue = messages as byte[][] ?? messages.ToArray()
        });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public IHealthCheckResult HealthCheck()
        => HealthCheckResult.FromProbe("PostgreSQL", _name, _logger, async ct =>
        {
            await using var conn = await OpenConnectionAsync(ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            _ = await cmd.ExecuteScalarAsync(ct);
        });

    public async ValueTask DisposeAsync()
    {
        await StopListeningAsync();
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
            _dataSource = null;
        }
        _schemaLock.Dispose();
    }

    /// <summary>
    /// Finalizes the previous batch and claims the next one in a single <see cref="NpgsqlBatch"/>: one round trip,
    /// executed by PostgreSQL as one implicit transaction (one commit / WAL flush per batch).
    /// </summary>
    private async Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var batch = conn.CreateBatch();

        if (outcomes.Count > 0)
            batch.BatchCommands.Add(CreateFinalizeCommand(outcomes));

        var claim = new NpgsqlBatchCommand(PostgresQueueSql.ClaimMessages(_settings.TableName));
        claim.Parameters.Add(new NpgsqlParameter<int>("batchSize", _settings.BatchSize));
        claim.Parameters.Add(new NpgsqlParameter<int>("leaseTimeout", LeasedQueueListener.GetLeaseTimeoutSeconds(_settings)));
        batch.BatchCommands.Add(claim);

        var claimed = new List<QueueMessage>();
        await using var reader = await batch.ExecuteReaderAsync(ct);
        do
        {
            // The finalize UPDATE returns no rows; the claim's RETURNING rows are the only result set with fields.
            if (reader.FieldCount == 0)
                continue;

            while (await reader.ReadAsync(ct))
            {
                claimed.Add(new QueueMessage(
                    QueueId: reader.GetInt64(0),
                    AttemptCount: reader.GetInt32(3),
                    Payload: reader.GetFieldValue<byte[]>(2),
                    MessageId: reader.GetGuid(1)));
            }
        }
        while (await reader.NextResultAsync(ct));

        return claimed;
    }

    private async Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var batch = conn.CreateBatch();
        batch.BatchCommands.Add(CreateFinalizeCommand(outcomes));
        await batch.ExecuteNonQueryAsync(ct);
    }

    private NpgsqlBatchCommand CreateFinalizeCommand(IReadOnlyList<QueueMessageOutcome> outcomes)
    {
        var cmd = new NpgsqlBatchCommand(PostgresQueueSql.FinalizeMessages(_settings.TableName));
        cmd.Parameters.Add(new NpgsqlParameter<long[]>("queueIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
        {
            TypedValue = outcomes.Select(static o => o.Message.QueueId).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter<int[]>("attempts", NpgsqlDbType.Array | NpgsqlDbType.Integer)
        {
            TypedValue = outcomes.Select(static o => o.Message.AttemptCount).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter<int[]>("statuses", NpgsqlDbType.Array | NpgsqlDbType.Integer)
        {
            TypedValue = outcomes.Select(static o => (int)o.Status).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter<string?[]>("errors", NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            TypedValue = outcomes.Select(static o => o.Error).ToArray()
        });
        return cmd;
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

    /// <summary>The shared queue loop, plus waiting for a NOTIFY instead of the full poll interval when idle.</summary>
    private sealed class Listener(PostgresEndPoint owner) : LeasedQueueListener(owner._name, owner._settings, owner._logger)
    {
        private NpgsqlConnection? _notificationConnection;

        protected override Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
            => owner.FinalizeAndClaimAsync(outcomes, ct);

        protected override Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
            => owner.FinalizeAsync(outcomes, ct);

        /// <summary>
        /// Waits for a NOTIFY (or <see cref="PostgresSettings.PollInterval"/>, whichever comes first). If the LISTEN
        /// connection cannot be opened or fails, falls back to a plain delay and retries LISTEN on the next idle cycle
        /// instead of terminating the loop.
        /// </summary>
        protected override async Task WaitWhenIdleAsync(CancellationToken ct)
        {
            if (!owner._settings.UseNotifications)
            {
                await Task.Delay(IdleDelay, ct);
                return;
            }

            try
            {
                _notificationConnection ??= await owner.OpenNotificationConnectionAsync(ct);
                using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                waitCts.CancelAfter(IdleDelay);
                await _notificationConnection.WaitAsync(waitCts.Token);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // Poll interval elapsed without a notification.
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogWarning(ex, "Notification wait failed on endpoint '{Name}', falling back to timer-based polling", EndpointName);
                await DisposeNotificationConnectionAsync();
                await Task.Delay(IdleDelay, ct);
            }
        }

        protected override ValueTask OnLoopExitAsync() => DisposeNotificationConnectionAsync();

        private async ValueTask DisposeNotificationConnectionAsync()
        {
            if (_notificationConnection is not null)
            {
                await _notificationConnection.DisposeAsync();
                _notificationConnection = null;
            }
        }
    }
}
