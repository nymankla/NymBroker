namespace NymBroker.Sql;

public sealed class SqliteSettings
{
    public string ConnectionString { get; set; } = "Data Source=messages.db";
    public string TableName        { get; set; } = "NymBrokerMessages";
    public int    BatchSize        { get; set; } = 10;
    public bool   AutoCreateTable  { get; set; } = true;
    public TimeSpan PollInterval   { get; set; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan LeaseTimeout   { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxRetryCount       { get; set; } = 5;

    /// <summary>
    /// When true (default), failures are settled in the table: retried until <see cref="MaxRetryCount"/>, and messages
    /// that can never succeed are marked Failed immediately. When false, the broker posts failures to its own
    /// dead-letter endpoint (<c>WithDeadLetterEndpoint</c>) and the row is marked Completed.
    /// </summary>
    public bool UseNativeDeadLetter { get; set; } = true;
}
