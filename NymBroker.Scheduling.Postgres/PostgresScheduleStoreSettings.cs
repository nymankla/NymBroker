using NymBroker.Shared;

namespace NymBroker.Scheduling.Postgres;

public sealed class PostgresScheduleStoreSettings
{
    /// <summary>Connection string for the PostgreSQL database used by this store.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Optionally schema-qualified table name. Brokers using the same table share their schedules.</summary>
    public string TableName { get; set; } = "nymbroker_schedule";

    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid. Runs at registration and in the store's constructor.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(ConnectionString));

        PostgresStoreSchema.ValidateTableName(TableName, maxParts: 2);
    }
}
