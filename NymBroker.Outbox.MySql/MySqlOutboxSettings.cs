using NymBroker.Shared;

namespace NymBroker.Outbox.MySql;

public sealed class MySqlOutboxSettings
{
    /// <summary>
    /// The database the outbox table is in: the one your transactions are on. MySQL 8.0+ or MariaDB 10.6+. Default matches
    /// the local container started by <c>scripts/setup-mysql.ps1</c>.
    /// </summary>
    public string ConnectionString { get; set; } = "Server=localhost;Port=3306;Database=nymbroker;User ID=root;Password=NymBroker!Dev123";

    /// <summary>Table name, optionally database-qualified. Every instance of the application uses the same table.</summary>
    public string TableName { get; set; } = "nymbroker_outbox";

    public bool AutoCreateTable { get; set; } = true;

    /// <summary>Throws <see cref="ArgumentException"/> if a setting is invalid. Runs at registration and in the store's constructor.</summary>
    public void Validate()
    {
        OutboxSettingsCheck.RequireConnectionString(ConnectionString);
        MySqlIdentifier.QuoteTableName(TableName);
    }
}
