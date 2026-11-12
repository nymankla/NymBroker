using NymBroker.Shared;

namespace NymBroker.Outbox.Postgres;

public sealed class PostgresOutboxSettings
{
    /// <summary>The database the outbox table is in: the one your transactions are on.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Table name, optionally schema-qualified. Every instance of the application uses the same table.</summary>
    public string TableName { get; set; } = "nymbroker_outbox";

    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid. Runs at registration and in the store's constructor.</summary>
    public void Validate()
    {
        OutboxSettingsCheck.RequireConnectionString(ConnectionString);
        PostgresStoreSchema.ValidateTableName(TableName);
    }
}
