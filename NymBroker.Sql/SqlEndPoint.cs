using System.Runtime.CompilerServices;
using System.Text;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Sql;

public sealed class SqlEndPoint : IEndPointPoll, IEndPointEventDriven, IAsyncDisposable
{
    private readonly SqlSettings _settings;
    private readonly ILogger<SqlEndPoint> _logger;
    // Serializes all DB operations — ensures single-connection SQLite is never accessed concurrently.
    private readonly SemaphoreSlim _dbLock = new(1, 1);
    private SqliteConnection? _connection;
    private CancellationTokenSource? _listeningCts;

    public string Name { get; }

    public SqlEndPoint(string name, SqlSettings settings, ILogger<SqlEndPoint> logger)
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
                        await foreach (var msg in ReadAsync(token))
                        {
                            try { await handler(msg, token); }
                            catch (Exception ex) when (ex is not OperationCanceledException)
                            { _logger.LogError(ex, "Unhandled error dispatching message on endpoint '{Name}'", Name); }
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

    // ── IEndPointPoll ───────────────────────────────────────────────────────

    public async IAsyncEnumerable<string> ReadAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // Claim the batch under the lock so concurrent PostAsync calls can't interleave
        // with the SELECT+UPDATE sequence on the shared SQLite connection.
        List<string> payloads;
        await _dbLock.WaitAsync(ct);
        try
        {
            var conn = await EnsureConnectionAsync(ct);

            var rows = (await conn.QueryAsync<PendingRow>(
                $"SELECT Id, Payload FROM {_settings.TableName} WHERE Status = 'Pending' ORDER BY CreatedAt LIMIT @BatchSize",
                new { _settings.BatchSize })).ToList();

            payloads = new List<string>(rows.Count);
            foreach (var row in rows)
            {
                if (ct.IsCancellationRequested) break;
                var claimed = await conn.ExecuteAsync(
                    $"UPDATE {_settings.TableName} SET Status = 'Processed', ProcessedAt = datetime('now') WHERE Id = @Id AND Status = 'Pending'",
                    new { row.Id });
                if (claimed > 0)
                    payloads.Add(row.Payload);
            }
        }
        finally
        {
            _dbLock.Release();
        }

        foreach (var payload in payloads)
        {
            if (ct.IsCancellationRequested) yield break;
            yield return payload;
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
                $"INSERT INTO {_settings.TableName} (Id, Status, CreatedAt, Payload) VALUES (@Id, 'Pending', datetime('now'), @Payload)",
                new { Id = Guid.NewGuid().ToString(), Payload = payload });
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
            _logger.LogError(ex, "SQL endpoint '{Name}' health check failed", Name);
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
        {
            await conn.ExecuteAsync($"""
                CREATE TABLE IF NOT EXISTS {_settings.TableName} (
                    Id          TEXT NOT NULL PRIMARY KEY,
                    Status      TEXT NOT NULL DEFAULT 'Pending',
                    CreatedAt   TEXT NOT NULL,
                    ProcessedAt TEXT NULL,
                    Payload     TEXT NOT NULL
                )
                """);
            await conn.ExecuteAsync($"""
                CREATE INDEX IF NOT EXISTS IX_{_settings.TableName}_Status
                    ON {_settings.TableName}(Status, CreatedAt)
                """);
        }

        _connection = conn;
        return _connection;
    }

    private sealed class PendingRow
    {
        public string Id      { get; set; } = "";
        public string Payload { get; set; } = "";
    }
}
