using NymBroker.Shared;

namespace NymBroker.Outbox.SqlServer;

public sealed class SqlServerOutboxSettings
{
    /// <summary>
    /// The database the outbox table is in: the one your transactions are on. Default matches the local container started
    /// by <c>scripts/setup-sqlserver.ps1</c>.
    /// </summary>
    public string ConnectionString { get; set; } = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True";

    /// <summary>Schema-qualified table name. Every instance of the application uses the same table.</summary>
    public string TableName { get; set; } = "dbo.nymbroker_outbox";

    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid. Runs at registration and in the store's constructor.</summary>
    public void Validate()
    {
        OutboxSettingsCheck.RequireConnectionString(ConnectionString);
        if (string.IsNullOrWhiteSpace(TableName) || TableName.Split('.').Any(string.IsNullOrWhiteSpace))
            throw new ArgumentException($"TableName '{TableName}' is not a valid (optionally schema-qualified) table name.", nameof(TableName));
    }
}
