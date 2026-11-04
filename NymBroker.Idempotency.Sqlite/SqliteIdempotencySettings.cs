using NymBroker.Shared;

namespace NymBroker.Idempotency.Sqlite;

public sealed class SqliteIdempotencySettings
{
    /// <summary>
    /// Connection string for the SQLite database. SQLite gives restart-safe deduplication on one host only;
    /// use SQL Server or PostgreSQL when several instances on different machines must share it.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Table name. Brokers on the same host using the same database file and table share duplicate detection.</summary>
    public string TableName { get; set; } = "nymbroker_idempotency";

    public TimeSpan Ttl { get; set; } = TimeSpan.FromHours(24);

    public TimeSpan LeaseTimeout { get; set; } = TimeSpan.FromMinutes(5);

    public bool AutoCreateTable { get; set; } = true;

    /// <summary>How often expired rows are deleted. <see cref="TimeSpan.Zero"/> disables the cleanup service.</summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Rows deleted per statement during cleanup.</summary>
    public int CleanupBatchSize { get; set; } = 1000;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(ConnectionString));

        SqliteStoreTable.Validate(TableName);

        ExpiringStore.ValidateTimes(Ttl, LeaseTimeout, CleanupInterval, CleanupBatchSize);
    }

    internal int TtlSeconds => (int)Math.Ceiling(Ttl.TotalSeconds);

    internal int LeaseSeconds => (int)Math.Ceiling(LeaseTimeout.TotalSeconds);
}
