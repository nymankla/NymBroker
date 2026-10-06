using System.Text;

namespace NymBroker.Idempotency.Postgres;

public sealed class PostgresIdempotencySettings
{
    /// <summary>Connection string for the PostgreSQL database used by this store.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Schema-qualified table name. Brokers using the same table share duplicate detection.</summary>
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

        if (string.IsNullOrWhiteSpace(TableName))
            throw new ArgumentException("TableName is required.", nameof(TableName));

        var parts = TableName.Split('.');
        if (parts.Any(string.IsNullOrWhiteSpace) ||
            parts.Any(part => Encoding.UTF8.GetByteCount(part.Trim()) > 63))
            throw new ArgumentException($"TableName '{TableName}' is not a valid PostgreSQL (optionally schema-qualified) table name.", nameof(TableName));

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
