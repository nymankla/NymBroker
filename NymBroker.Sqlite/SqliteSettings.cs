namespace NymBroker.Sql;

public sealed class SqliteSettings
{
    public string ConnectionString { get; set; } = "Data Source=messages.db";

    /// <summary>Letters, digits and underscores, not starting with a digit (validated; always quoted in SQL).</summary>
    public string TableName        { get; set; } = "NymBrokerMessages";
    public int    BatchSize        { get; set; } = 10;
    public bool   AutoCreateTable  { get; set; } = true;

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

    /// <summary>WAL (default): readers don't block the writer; commits are sequential appends. Not applied to in-memory databases.</summary>
    public SqliteJournalMode JournalMode { get; set; } = SqliteJournalMode.Wal;

    /// <summary>
    /// Normal (default) under WAL: survives an application crash; the last commits can be lost on power loss.
    /// Use <see cref="SqliteSynchronous.Full"/> for strict durability. Not applied to in-memory databases.
    /// </summary>
    public SqliteSynchronous Synchronous { get; set; } = SqliteSynchronous.Normal;

    /// <summary>How long a write waits for another connection's lock before failing with SQLITE_BUSY.</summary>
    public TimeSpan BusyTimeout { get; set; } = TimeSpan.FromSeconds(5);
}

/// <summary>SQLite <c>PRAGMA journal_mode</c>.</summary>
public enum SqliteJournalMode { Wal, Delete, Truncate, Persist, Memory, Off }

/// <summary>SQLite <c>PRAGMA synchronous</c>.</summary>
public enum SqliteSynchronous { Off, Normal, Full, Extra }
