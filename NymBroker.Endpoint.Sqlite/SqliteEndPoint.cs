using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Endpoint.Sqlite;

/// <summary>
/// Queue table in SQLite. One connection, serialized by <see cref="_dbLock"/> (a <see cref="SqliteConnection"/> is not
/// safe for concurrent use), with every statement prepared once. The listener claims a batch with one
/// <c>UPDATE … RETURNING</c>, and writes the batch's outcomes in the same transaction as the next claim. The loop itself
/// is the shared <see cref="LeasedQueueListener"/>.
/// </summary>
public sealed class SqliteEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private const string ReadAsyncLeaseReturnedMessage = "Message lease returned by ReadAsync without acknowledgement.";

    private readonly SqliteSettings _settings;
    private readonly ILogger<SqliteEndPoint> _logger;
    private readonly string _name;
    // Serializes all DB operations — ensures single-connection SQLite is never accessed concurrently.
    private readonly SemaphoreSlim _dbLock = new(1, 1);
    private SqliteConnection? _connection;
    private PreparedCommands? _commands;
    private readonly Listener _listener;
    private bool _disposed;

    public EndpointMode Mode { get; }

    public SqliteEndPoint(string name, SqliteSettings settings, ILogger<SqliteEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
        SqliteQueueSql.ValidateTableName(settings.TableName);
        _name = name;
        Mode = mode;
        _settings = settings;
        _logger = logger;
        _listener = new Listener(this);
    }

    // ── IEndPointEventDriven ────────────────────────────────────────────────

    /// <summary>Dead-letters by marking the row <c>Failed</c> immediately, with the reason in <c>LastError</c>.</summary>
    public bool UsesNativeDeadLetter => _settings.UseNativeDeadLetter;

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        _listener.Start(handler, ct);
        return Task.CompletedTask;
    }

    /// <summary>Stops polling and waits for the loop to finish, so no handler runs after this returns.</summary>
    public Task StopListeningAsync() => _listener.StopAsync();

    /// <summary>
    /// Claims one batch and yields its payloads (UTF-8 decoded). An item is completed when the caller moves past it,
    /// and returned to Pending if the enumeration stops early.
    /// </summary>
    public async IAsyncEnumerable<string> ReadAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var messages = await FinalizeAndClaimAsync([], claim: true, ct);
        foreach (var message in messages)
        {
            if (ct.IsCancellationRequested) yield break;

            var completed = false;
            try
            {
                yield return Encoding.UTF8.GetString(message.Payload);
                completed = true;
            }
            finally
            {
                var outcome = completed
                    ? QueueMessageOutcome.Completed(message)
                    : new QueueMessageOutcome(message, QueueMessageStatus.Pending, ReadAsyncLeaseReturnedMessage);
                await FinalizeAndClaimAsync([outcome], claim: false, CancellationToken.None);
            }
        }
    }

    // ── IEndPoint ───────────────────────────────────────────────────────────

    /// <summary>All messages in one transaction (atomic, order preserved).</summary>
    public async Task PostBatchAsync(IReadOnlyList<byte[]> messages, CancellationToken ct = default)
    {
        if (messages.Count == 0) return;

        await _dbLock.WaitAsync(ct);
        try
        {
            var (conn, commands) = await EnsureConnectionAsync(ct);
            await using var tx = BeginImmediate(conn);
            commands.Insert.Transaction = tx;
            try
            {
                foreach (var message in messages)
                    await ExecuteInsertAsync(commands, message, ct);
                await tx.CommitAsync(ct);
            }
            finally
            {
                commands.Insert.Transaction = null;
            }
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            var (_, commands) = await EnsureConnectionAsync(ct);
            await ExecuteInsertAsync(commands, message, ct);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public IHealthCheckResult HealthCheck()
        => HealthCheckResult.FromProbe("SQLite", _name, _logger, async ct =>
        {
            if (_connection?.State == System.Data.ConnectionState.Open)
                return;

            await using var probe = new SqliteConnection(_settings.ConnectionString);
            await probe.OpenAsync(ct);
            await using var command = probe.CreateCommand();
            command.CommandText = "SELECT 1";
            await command.ExecuteScalarAsync(ct);
        });

    // ── IAsyncDisposable ────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await StopListeningAsync();
        _commands?.Dispose();
        _commands = null;
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
        _dbLock.Dispose();
    }

    // ── Data access ─────────────────────────────────────────────────────────

    /// <summary>
    /// Writes <paramref name="outcomes"/> and (when <paramref name="claim"/>) claims the next batch, in one
    /// <c>BEGIN IMMEDIATE</c> transaction: one commit per batch instead of one per message.
    /// </summary>
    private async Task<List<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, bool claim, CancellationToken ct)
    {
        if (outcomes.Count == 0 && !claim) return [];

        await _dbLock.WaitAsync(ct);
        try
        {
            var (conn, commands) = await EnsureConnectionAsync(ct);
            await using var tx = BeginImmediate(conn);
            commands.SetTransaction(tx);
            try
            {
                foreach (var outcome in outcomes)
                    await ExecuteFinalizeAsync(commands, outcome, ct);

                var claimed = claim ? await ExecuteClaimAsync(commands, ct) : [];
                await tx.CommitAsync(ct);
                return claimed;
            }
            finally
            {
                commands.SetTransaction(null);
            }
        }
        finally
        {
            _dbLock.Release();
        }
    }

    private static async Task ExecuteInsertAsync(PreparedCommands commands, byte[] message, CancellationToken ct)
    {
        commands.InsertMessageId.Value = Guid.NewGuid().ToByteArray();
        commands.InsertPayload.Value = message;
        await commands.Insert.ExecuteNonQueryAsync(ct);
    }

    private async Task<List<QueueMessage>> ExecuteClaimAsync(PreparedCommands commands, CancellationToken ct)
    {
        commands.ClaimLeaseSeconds.Value = LeasedQueueListener.GetLeaseTimeoutSeconds(_settings);
        commands.ClaimBatchSize.Value = _settings.BatchSize;

        var claimed = new List<QueueMessage>(_settings.BatchSize);
        await using (var reader = await commands.Claim.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                claimed.Add(new QueueMessage(
                    QueueId: reader.GetInt64(0),
                    AttemptCount: reader.GetInt32(3),
                    Payload: reader.GetFieldValue<byte[]>(2),
                    MessageId: MessageIdText(reader.GetFieldValue<byte[]>(1))));
        }

        // RETURNING doesn't guarantee order; handle in queue order.
        claimed.Sort(static (a, b) => a.QueueId.CompareTo(b.QueueId));
        return claimed;
    }

    private static async Task ExecuteFinalizeAsync(PreparedCommands commands, QueueMessageOutcome outcome, CancellationToken ct)
    {
        var (command, id, attempt, error) = outcome.Status switch
        {
            QueueMessageStatus.Completed => (commands.MarkCompleted, commands.CompletedId, commands.CompletedAttempt, null),
            QueueMessageStatus.Failed => (commands.MarkFailed, commands.FailedId, commands.FailedAttempt, commands.FailedError),
            _ => (commands.MarkPending, commands.PendingId, commands.PendingAttempt, commands.PendingError)
        };

        id.Value = outcome.Message.QueueId;
        attempt.Value = outcome.Message.AttemptCount;
        if (error is not null) error.Value = (object?)outcome.Error ?? DBNull.Value;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static SqliteTransaction BeginImmediate(SqliteConnection conn)
        => conn.BeginTransaction(deferred: false);   // BEGIN IMMEDIATE: take the write lock up front

    // Must be called while holding _dbLock.
    private async Task<(SqliteConnection Connection, PreparedCommands Commands)> EnsureConnectionAsync(CancellationToken ct)
    {
        if (_connection is not null && _commands is not null) return (_connection, _commands);

        var conn = new SqliteConnection(_settings.ConnectionString);
        try
        {
            await conn.OpenAsync(ct);
            await ApplyPragmasAsync(conn, ct);

            if (_settings.AutoCreateTable)
                await EnsureSchemaAsync(conn, ct);

            _commands = PreparedCommands.Create(conn, _settings.TableName);
            _connection = conn;
            return (_connection, _commands);
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    private async Task ApplyPragmasAsync(SqliteConnection conn, CancellationToken ct)
    {
        var pragmas = new StringBuilder();
        if (!IsInMemory(_settings.ConnectionString))
        {
            pragmas.Append($"PRAGMA journal_mode = {_settings.JournalMode.ToString().ToUpperInvariant()};");
            pragmas.Append($"PRAGMA synchronous = {_settings.Synchronous.ToString().ToUpperInvariant()};");
        }
        pragmas.Append($"PRAGMA busy_timeout = {(long)Math.Max(0, _settings.BusyTimeout.TotalMilliseconds)};");
        pragmas.Append("PRAGMA temp_store = MEMORY;");

        await using var command = conn.CreateCommand();
        command.CommandText = pragmas.ToString();
        await command.ExecuteNonQueryAsync(ct);
    }

    internal static bool IsInMemory(string connectionString)
    {
        var builder = new SqliteConnectionStringBuilder(connectionString);
        return builder.Mode == SqliteOpenMode.Memory
               || string.Equals(builder.DataSource, ":memory:", StringComparison.OrdinalIgnoreCase)
               || string.IsNullOrEmpty(builder.DataSource);
    }

    // ── Schema ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates the table, or migrates an older one. The version is detected per table from its columns
    /// (<c>PRAGMA user_version</c> is per database file, and one file may hold several endpoint tables);
    /// <c>user_version</c> is still set to 2 once a table is at the current schema.
    /// </summary>
    private async Task EnsureSchemaAsync(SqliteConnection conn, CancellationToken ct)
    {
        var columns = await GetColumnTypesAsync(conn, _settings.TableName, ct);
        if (columns.Count == 0)
        {
            await ExecuteAsync(conn, SqliteQueueSql.CreateTable(_settings.TableName), null, ct);
            return;
        }

        var isCurrent = columns.TryGetValue("Payload", out var payloadType)
                        && string.Equals(payloadType, "BLOB", StringComparison.OrdinalIgnoreCase)
                        && columns.ContainsKey("QueueId");
        if (isCurrent)
        {
            await ExecuteAsync(conn, SqliteQueueSql.CreateTable(_settings.TableName), null, ct);   // idempotent: index + user_version
            return;
        }

        var fromVersion1 = columns.ContainsKey("QueueId") && columns.ContainsKey("MessageId");
        await MigrateAsync(conn, fromVersion1, ct);
    }

    private async Task MigrateAsync(SqliteConnection conn, bool fromVersion1, CancellationToken ct)
    {
        var table = _settings.TableName;
        var backup = $"{table}_{(fromVersion1 ? "v1" : "Legacy")}_{DateTime.UtcNow:yyyyMMddHHmmss}";

        await using var tx = BeginImmediate(conn);
        await ExecuteAsync(conn, $"ALTER TABLE {SqliteQueueSql.Quote(table)} RENAME TO {SqliteQueueSql.Quote(backup)}", tx, ct);
        await ExecuteAsync(conn, SqliteQueueSql.DropVersion1Indexes(table), tx, ct);
        await ExecuteAsync(conn, SqliteQueueSql.CreateTable(table), tx, ct);
        await ExecuteAsync(conn, fromVersion1
            ? SqliteQueueSql.CopyFromVersion1(table, backup)
            : SqliteQueueSql.CopyFromLegacy(table, backup), tx, ct);
        await tx.CommitAsync(ct);

        _logger.LogInformation("Migrated SQLite queue table '{Table}' to schema version {Version}; the previous table was kept as '{Backup}'",
            table, SqliteQueueSql.SchemaVersion, backup);
    }

    private static async Task<Dictionary<string, string>> GetColumnTypesAsync(SqliteConnection conn, string table, CancellationToken ct)
    {
        var columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        await using var command = conn.CreateCommand();
        command.CommandText = "SELECT name, type FROM pragma_table_info($table)";
        command.Parameters.AddWithValue("$table", table);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            columns[reader.GetString(0)] = reader.GetString(1);
        return columns;
    }

    private static async Task ExecuteAsync(SqliteConnection conn, string sql, SqliteTransaction? tx, CancellationToken ct)
    {
        await using var command = conn.CreateCommand();
        command.CommandText = sql;
        command.Transaction = tx;
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>16 bytes → the GUID; anything else (rows migrated from a TEXT id) → UTF-8 text. Only logged.</summary>
    private static string MessageIdText(byte[] messageId)
        => messageId.Length == 16 ? new Guid(messageId).ToString() : Encoding.UTF8.GetString(messageId);

    // ── Types ───────────────────────────────────────────────────────────────

    /// <summary>The shared queue loop over this endpoint's finalize-and-claim transaction.</summary>
    private sealed class Listener(SqliteEndPoint owner) : LeasedQueueListener(owner._name, owner._settings, owner._logger)
    {
        protected override async Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
            => await owner.FinalizeAndClaimAsync(outcomes, claim: true, ct);

        protected override Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
            => owner.FinalizeAndClaimAsync(outcomes, claim: false, ct);
    }

    /// <summary>Every statement prepared once on the endpoint's connection; only parameter values change per call.</summary>
    private sealed class PreparedCommands : IDisposable
    {
        public required SqliteCommand Insert { get; init; }
        public required SqliteParameter InsertMessageId { get; init; }
        public required SqliteParameter InsertPayload { get; init; }

        public required SqliteCommand Claim { get; init; }
        public required SqliteParameter ClaimLeaseSeconds { get; init; }
        public required SqliteParameter ClaimBatchSize { get; init; }

        public required SqliteCommand MarkCompleted { get; init; }
        public required SqliteParameter CompletedId { get; init; }
        public required SqliteParameter CompletedAttempt { get; init; }

        public required SqliteCommand MarkFailed { get; init; }
        public required SqliteParameter FailedId { get; init; }
        public required SqliteParameter FailedAttempt { get; init; }
        public required SqliteParameter FailedError { get; init; }

        public required SqliteCommand MarkPending { get; init; }
        public required SqliteParameter PendingId { get; init; }
        public required SqliteParameter PendingAttempt { get; init; }
        public required SqliteParameter PendingError { get; init; }

        public static PreparedCommands Create(SqliteConnection conn, string table)
        {
            var insert = Command(conn, SqliteQueueSql.Insert(table));
            var claim = Command(conn, SqliteQueueSql.Claim(table));
            var completed = Command(conn, SqliteQueueSql.MarkCompleted(table));
            var failed = Command(conn, SqliteQueueSql.MarkFailed(table));
            var pending = Command(conn, SqliteQueueSql.MarkPending(table));

            var commands = new PreparedCommands
            {
                Insert = insert,
                InsertMessageId = Parameter(insert, "$messageId", SqliteType.Blob),
                InsertPayload = Parameter(insert, "$payload", SqliteType.Blob),
                Claim = claim,
                ClaimLeaseSeconds = Parameter(claim, "$leaseSeconds", SqliteType.Integer),
                ClaimBatchSize = Parameter(claim, "$batchSize", SqliteType.Integer),
                MarkCompleted = completed,
                CompletedId = Parameter(completed, "$id", SqliteType.Integer),
                CompletedAttempt = Parameter(completed, "$attempt", SqliteType.Integer),
                MarkFailed = failed,
                FailedId = Parameter(failed, "$id", SqliteType.Integer),
                FailedAttempt = Parameter(failed, "$attempt", SqliteType.Integer),
                FailedError = Parameter(failed, "$error", SqliteType.Text),
                MarkPending = pending,
                PendingId = Parameter(pending, "$id", SqliteType.Integer),
                PendingAttempt = Parameter(pending, "$attempt", SqliteType.Integer),
                PendingError = Parameter(pending, "$error", SqliteType.Text)
            };

            foreach (var command in commands.All)
                command.Prepare();
            return commands;
        }

        private IEnumerable<SqliteCommand> All => [Insert, Claim, MarkCompleted, MarkFailed, MarkPending];

        public void SetTransaction(SqliteTransaction? tx)
        {
            foreach (var command in All)
                command.Transaction = tx;
        }

        public void Dispose()
        {
            foreach (var command in All)
                command.Dispose();
        }

        private static SqliteCommand Command(SqliteConnection conn, string sql)
        {
            var command = conn.CreateCommand();
            command.CommandText = sql;
            return command;
        }

        private static SqliteParameter Parameter(SqliteCommand command, string name, SqliteType type)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.SqliteType = type;
            parameter.Value = DBNull.Value;
            command.Parameters.Add(parameter);
            return parameter;
        }
    }
}
