using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace NymBroker.Shared;

/// <summary>
/// One persistent connection (also required for <c>Data Source=:memory:</c>), serialized by a semaphore, as in
/// <c>SqliteEndPoint</c>. File databases use WAL mode and a busy timeout so several processes on the host can share them.
/// </summary>
internal sealed class SqliteStoreConnection(string connectionString, string? createSchemaSql, ILogger logger, string description) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private SqliteConnection? _connection;
    private bool _disposed;

    /// <summary>Waits for the connection, opening it if needed. Keep the lease for as long as the connection is in use.</summary>
    public async Task<Lease> AcquireAsync(CancellationToken ct)
    {
        await _lock.WaitAsync(ct);
        try
        {
            return new Lease(await EnsureOpenAsync(ct), _lock);
        }
        catch
        {
            _lock.Release();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _lock.WaitAsync();
        try
        {
            if (_disposed) return;
            _disposed = true;
            if (_connection is not null)
                await _connection.DisposeAsync();
            _connection = null;
        }
        finally
        {
            _lock.Release();
        }
    }

    // Caller holds _lock.
    private async Task<SqliteConnection> EnsureOpenAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connection is not null) return _connection;

        var builder = new SqliteConnectionStringBuilder(connectionString);
        var inMemory = builder.Mode == SqliteOpenMode.Memory
                       || string.IsNullOrEmpty(builder.DataSource)
                       || builder.DataSource.Equals(":memory:", StringComparison.OrdinalIgnoreCase);

        var connection = new SqliteConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct);
            await using (var pragma = connection.CreateCommand())
            {
                pragma.CommandText = inMemory
                    ? "PRAGMA busy_timeout=5000;"
                    : "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;";
                await pragma.ExecuteNonQueryAsync(ct);
            }

            if (createSchemaSql is not null)
            {
                await using var create = connection.CreateCommand();
                create.CommandText = createSchemaSql;
                await create.ExecuteNonQueryAsync(ct);
                logger.LogDebug("{Description} is ready", description);
            }
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }

        _connection = connection;
        return connection;
    }

    /// <summary>The connection, held exclusively until this is disposed.</summary>
    public readonly struct Lease(SqliteConnection connection, SemaphoreSlim held) : IDisposable
    {
        public SqliteConnection Connection { get; } = connection;
        public void Dispose() => held.Release();
    }
}
