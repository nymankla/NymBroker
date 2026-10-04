namespace NymBroker.SqlServer;

public sealed class SqlServerSettings
{
    /// <summary>Default matches the local container started by <c>scripts/setup-sqlserver.ps1</c>.</summary>
    public string ConnectionString { get; set; } = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True";
    public string TableName        { get; set; } = "dbo.nymbroker_messages";
    public int BatchSize           { get; set; } = 10;
    public bool AutoCreateTable    { get; set; } = true;
    /// <summary>Delay after a poll that found no messages. While messages are waiting, batches are claimed back to back.</summary>
    public TimeSpan PollInterval   { get; set; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan LeaseTimeout   { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxRetryCount       { get; set; } = 5;
}
