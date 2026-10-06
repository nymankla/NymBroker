namespace NymBroker.Idempotency.SqlServer;

public sealed class SqlServerIdempotencySettings
{
    /// <summary>Default matches the local container started by <c>scripts/setup-sqlserver.ps1</c>.</summary>
    public string ConnectionString { get; set; } = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True";

    /// <summary>Schema-qualified table name. Several brokers that should share duplicate detection use the same table.</summary>
    public string TableName { get; set; } = "dbo.nymbroker_idempotency";

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
        if (string.IsNullOrWhiteSpace(TableName) || TableName.Split('.').Any(string.IsNullOrWhiteSpace))
            throw new ArgumentException($"TableName '{TableName}' is not a valid (optionally schema-qualified) table name.", nameof(TableName));
        if (Ttl < TimeSpan.FromSeconds(1) || Ttl.TotalSeconds > int.MaxValue)
            throw new ArgumentException("Ttl must be at least 1 second.", nameof(Ttl));
        if (LeaseTimeout < TimeSpan.FromSeconds(1) || LeaseTimeout.TotalSeconds > int.MaxValue)
            throw new ArgumentException("LeaseTimeout must be at least 1 second.", nameof(LeaseTimeout));
        if (CleanupInterval < TimeSpan.Zero)
            throw new ArgumentException("CleanupInterval cannot be negative.", nameof(CleanupInterval));
        if (CleanupBatchSize < 1)
            throw new ArgumentException("CleanupBatchSize must be at least 1.", nameof(CleanupBatchSize));
    }

    internal int TtlSeconds => (int)Math.Ceiling(Ttl.TotalSeconds);
    internal int LeaseSeconds => (int)Math.Ceiling(LeaseTimeout.TotalSeconds);
}
