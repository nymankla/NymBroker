namespace NymBroker.Scheduling.SqlServer;

public sealed class SqlServerScheduleStoreSettings
{
    /// <summary>Default matches the local container started by <c>scripts/setup-sqlserver.ps1</c> (development only).</summary>
    public string ConnectionString { get; set; } = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True";

    /// <summary>Schema-qualified table name. Brokers using the same table share their schedules.</summary>
    public string TableName { get; set; } = "dbo.nymbroker_schedule";

    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid. Runs at registration and in the store's constructor.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(ConnectionString));
        if (string.IsNullOrWhiteSpace(TableName) || TableName.Split('.').Any(string.IsNullOrWhiteSpace))
            throw new ArgumentException($"TableName '{TableName}' is not a valid (optionally schema-qualified) table name.", nameof(TableName));
    }
}
