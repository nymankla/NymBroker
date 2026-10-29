namespace NymBroker.Scheduling.MySql;

public sealed class MySqlScheduleStoreSettings
{
    /// <summary>Default matches the local container started by <c>scripts/setup-mysql.ps1</c> (development only).</summary>
    public string ConnectionString { get; set; } = "Server=localhost;Port=3306;Database=nymbroker;User ID=root;Password=NymBroker!Dev123";

    /// <summary>Table name, optionally database-qualified. Brokers using the same table share their schedules.</summary>
    public string TableName { get; set; } = "nymbroker_schedule";

    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid. Runs at registration and in the store's constructor.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(ConnectionString));
        MySqlScheduleSql.QuoteTableName(TableName);
    }
}
