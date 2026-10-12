using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Endpoint.MySql;

public sealed class MySqlSettings : ILeasedQueueSettings
{
    /// <summary>Default matches the local container started by <c>scripts/setup-mysql.ps1</c> (development only).</summary>
    public string ConnectionString { get; set; } = "Server=localhost;Port=3306;Database=nymbroker;User ID=root;Password=NymBroker!Dev123";

    /// <summary>Table name, optionally database-qualified (<c>db.table</c>). Letters, digits, <c>_</c> and <c>$</c>.</summary>
    public string TableName        { get; set; } = "nymbroker_messages";
    public int BatchSize           { get; set; } = 10;
    public bool AutoCreateTable    { get; set; } = true;
    /// <summary>Delay after a poll that found no messages. While messages are waiting, batches are claimed back to back.</summary>
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
