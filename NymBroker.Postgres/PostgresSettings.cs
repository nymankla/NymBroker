namespace NymBroker.Postgres;

public sealed class PostgresSettings
{
    public string ConnectionString { get; set; } = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres";
    public string TableName        { get; set; } = "nymbroker_messages";
    public int BatchSize           { get; set; } = 10;
    public bool AutoCreateTable    { get; set; } = true;
    /// <summary>
    /// How long to wait after a poll that found no messages (cut short by a NOTIFY when <see cref="UseNotifications"/>
    /// is on). While messages are waiting, batches are claimed back to back.
    /// </summary>
    public TimeSpan PollInterval   { get; set; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan LeaseTimeout   { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxRetryCount       { get; set; } = 5;
    public bool UseNotifications   { get; set; } = true;
}
