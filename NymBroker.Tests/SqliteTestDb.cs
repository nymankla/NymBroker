using Microsoft.Data.Sqlite;

namespace NymBroker.Tests;

/// <summary>Small ADO.NET helpers for SQLite test assertions (replaces Dapper in the tests).</summary>
internal static class SqliteTestDb
{
    public static async Task<T> ScalarAsync<T>(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return value is null or DBNull ? default! : (T)Convert.ChangeType(value, Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T));
    }

    public static async Task<T> ScalarAsync<T>(string connectionString, string sql)
    {
        await using var connection = await OpenAsync(connectionString);
        return await ScalarAsync<T>(connection, sql);
    }

    public static async Task<int> ExecuteAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public static async Task<int> ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = await OpenAsync(connectionString);
        return await ExecuteAsync(connection, sql);
    }

    public static async Task<List<object?[]>> QueryAsync(string connectionString, string sql)
    {
        await using var connection = await OpenAsync(connectionString);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var rows = new List<object?[]>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++)
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }
        return rows;
    }

    public static async Task<SqliteConnection> OpenAsync(string connectionString)
    {
        var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    /// <summary>A temp file database; <see cref="Dispose"/> deletes it with its -wal / -shm files.</summary>
    public sealed class TempFile : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"nymbroker-sqlite-{Guid.NewGuid():N}.db");
        public string ConnectionString => $"Data Source={Path}";

        public void Dispose()
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in new[] { Path, Path + "-wal", Path + "-shm" })
            {
                try { File.Delete(file); }
                catch (IOException) { /* best effort: a connection may still hold the file briefly */ }
                catch (UnauthorizedAccessException) { /* best effort */ }
            }
        }
    }
}
