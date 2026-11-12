using Microsoft.Data.Sqlite;
using NymBroker.Shared;

namespace NymBroker.Outbox.Sqlite;

public sealed class SqliteOutboxSettings
{
    /// <summary>
    /// The database file the outbox table is in: the one your transactions are on. Not <c>:memory:</c>, since your
    /// connection and the dispatcher's must see the same database. Processes on one host may share the file.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Table name.</summary>
    public string TableName { get; set; } = "nymbroker_outbox";

    /// <summary>
    /// Create the table if it is missing: on the dispatcher's connection, and in the caller's transaction before each write,
    /// so a write never waits for the dispatcher's connection while the caller holds SQLite's write lock.
    /// </summary>
    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid. Runs at registration and in the store's constructor.</summary>
    public void Validate()
    {
        OutboxSettingsCheck.RequireConnectionString(ConnectionString);
        SqliteStoreTable.Validate(TableName);

        var builder = new SqliteConnectionStringBuilder(ConnectionString);
        if (builder.Mode == SqliteOpenMode.Memory || string.IsNullOrEmpty(builder.DataSource)
            || builder.DataSource.Equals(":memory:", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("The outbox needs a database file: an in-memory database is not shared with the dispatcher's connection.", nameof(ConnectionString));
    }
}
