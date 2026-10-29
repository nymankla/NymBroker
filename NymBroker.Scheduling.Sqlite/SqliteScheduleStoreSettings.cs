namespace NymBroker.Scheduling.Sqlite;

public sealed class SqliteScheduleStoreSettings
{
    /// <summary>
    /// Connection string for the SQLite database, e.g. <c>Data Source=nymbroker.db</c>. SQLite remembers runs across
    /// restarts on one host only; use SQL Server, PostgreSQL or MySQL when instances on several machines share the schedule.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Table name. Brokers on the same host using the same database file and table share their schedules.</summary>
    public string TableName { get; set; } = "nymbroker_schedule";

    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid. Runs at registration and in the store's constructor.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(ConnectionString));
        if (string.IsNullOrWhiteSpace(TableName) || TableName.Any(char.IsControl))
            throw new ArgumentException($"TableName '{TableName}' is not a valid SQLite table name.", nameof(TableName));
    }
}
