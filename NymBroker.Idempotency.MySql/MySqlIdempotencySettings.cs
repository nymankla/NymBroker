using NymBroker.Shared;

namespace NymBroker.Idempotency.MySql;

public sealed class MySqlIdempotencySettings
{
    /// <summary>Default matches the local container started by <c>scripts/setup-mysql.ps1</c> (development only).</summary>
    public string ConnectionString { get; set; } = "Server=localhost;Port=3306;Database=nymbroker;User ID=root;Password=NymBroker!Dev123";

    /// <summary>Table name, optionally database-qualified. Several brokers that should share duplicate detection use the same table.</summary>
    public string TableName { get; set; } = "nymbroker_idempotency";

    /// <summary>How long a processed message ID is remembered. Whole seconds (rounded up).</summary>
    public TimeSpan Ttl { get; set; } = TimeSpan.FromHours(24);

    /// <summary>How long a claim lasts if it is never completed or released (the process died). Whole seconds (rounded up).</summary>
    public TimeSpan LeaseTimeout { get; set; } = TimeSpan.FromMinutes(5);

    public bool AutoCreateTable { get; set; } = true;

    /// <summary>How often expired rows are deleted. <see cref="TimeSpan.Zero"/> disables the cleanup service.</summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Rows deleted per statement during cleanup (keeps each delete transaction short).</summary>
    public int CleanupBatchSize { get; set; } = 1000;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid. Runs at registration and in the store's constructor.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(ConnectionString));
        MySqlIdempotencySql.QuoteTableName(TableName);
        ExpiringStore.ValidateTimes(Ttl, LeaseTimeout, CleanupInterval, CleanupBatchSize);
    }

    internal int TtlSeconds => (int)Math.Ceiling(Ttl.TotalSeconds);
    internal int LeaseSeconds => (int)Math.Ceiling(LeaseTimeout.TotalSeconds);
}
