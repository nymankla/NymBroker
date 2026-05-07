namespace NymBroker.Sql;

public sealed class SqlSettings
{
    public string ConnectionString { get; set; } = "Data Source=messages.db";
    public string TableName        { get; set; } = "NymBrokerMessages";
    public int    BatchSize        { get; set; } = 10;
    public bool   AutoCreateTable  { get; set; } = true;
    public TimeSpan PollInterval   { get; set; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan LeaseTimeout   { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxRetryCount       { get; set; } = 5;
}
