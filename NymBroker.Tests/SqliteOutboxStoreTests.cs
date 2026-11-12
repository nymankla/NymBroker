using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.DI;
using NymBroker.Core.Outbox;
using NymBroker.Outbox.Sqlite;

namespace NymBroker.Tests;

/// <summary>The outbox contract on SQLite (always runs, on a temporary database file), plus settings and registration.</summary>
public sealed class SqliteOutboxStoreTests : OutboxStoreContractTests
{
    private readonly string _filePath = Path.Combine(Path.GetTempPath(), $"nb_outbox_{Guid.NewGuid():N}.db");

    internal string ConnectionString => $"Data Source={_filePath};Pooling=False";

    protected override bool ClaimSkipsOpenTransactions => false;

    protected override void RequireDatabase() { }

    protected override IOutboxStore CreateStore()
        => new SqliteOutboxStore(new SqliteOutboxSettings { ConnectionString = ConnectionString, TableName = TableName },
            NullLogger<SqliteOutboxStore>.Instance);

    protected override async Task<DbConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        await using var pragma = connection.CreateCommand();
        pragma.CommandText = "PRAGMA busy_timeout=5000;";
        await pragma.ExecuteNonQueryAsync(Ct);
        return connection;
    }

    protected override ValueTask DropTableAsync()
    {
        SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
            if (File.Exists(_filePath + suffix))
                File.Delete(_filePath + suffix);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void InvalidSettings_Throw()
    {
        Assert.Throws<ArgumentException>(() => new SqliteOutboxSettings { ConnectionString = " " }.Validate());
        Assert.Throws<ArgumentException>(() => new SqliteOutboxSettings { ConnectionString = "Data Source=:memory:" }.Validate());
        Assert.Throws<ArgumentException>(() => new SqliteOutboxSettings { ConnectionString = "Data Source=x;Mode=Memory" }.Validate());
        Assert.Throws<ArgumentException>(() => new SqliteOutboxSettings { ConnectionString = "Data Source=x.db", TableName = " " }.Validate());
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddNymBroker()
            .AddSqliteOutbox(new SqliteOutboxSettings { ConnectionString = "Data Source=x.db" }, o => o.BatchSize = 0));
    }

    [Fact]
    public void Sql_QuotesTableName()
        => Assert.Contains("\"my\"\"table\"", SqliteOutboxSql.Statements("my\"table").Claim);

    [Fact]
    public async Task AddSqliteOutbox_RegistersOutboxAndDispatcher_AfterTheBroker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddSqliteOutbox(new SqliteOutboxSettings { ConnectionString = ConnectionString }).Build();
        await using var provider = services.BuildServiceProvider();

        Assert.IsType<SqliteOutboxStore>(provider.GetRequiredService<IOutboxStore>());
        Assert.NotNull(provider.GetRequiredService<IOutbox>());
        var hosted = provider.GetServices<IHostedService>().ToList();
        Assert.True(hosted.FindIndex(s => s is OutboxDispatcher) > hosted.FindIndex(s => s is NymBrokerHostedService),
            "The dispatcher must start after the broker and stop before it.");
    }
}
