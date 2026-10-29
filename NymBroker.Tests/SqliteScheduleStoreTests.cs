using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.DI;
using NymBroker.Core.Factory;
using NymBroker.Core.Scheduling;
using NymBroker.Scheduling.Sqlite;

namespace NymBroker.Tests;

/// <summary>SQLite schedule store: always runs, on a temporary database file (stores on one file share it).</summary>
public sealed class SqliteScheduleStoreTests : ScheduleStoreContractTests
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"nb_sched_{Guid.NewGuid():N}.db");

    private SqliteScheduleStoreSettings Settings() => new() { ConnectionString = $"Data Source={_path}", TableName = TableName };

    protected override void RequireDatabase() { }

    protected override IScheduleStore CreateStore() => new SqliteScheduleStore(Settings(), NullLogger<SqliteScheduleStore>.Instance);

    protected override NymBrokerBuilder AddStore(NymBrokerBuilder builder) => builder.AddSqliteScheduleStore(Settings());

    protected override ValueTask DropTableAsync()
    {
        SqliteConnection.ClearAllPools();
        foreach (var file in new[] { _path, _path + "-wal", _path + "-shm" })
            File.Delete(file);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void InvalidSettings_Throw_AlsoAtRegistration()
    {
        Assert.Throws<ArgumentException>(new SqliteScheduleStoreSettings().Validate);   // no connection string
        Assert.Throws<ArgumentException>(new SqliteScheduleStoreSettings { ConnectionString = "Data Source=x.db", TableName = " " }.Validate);
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddNymBroker().AddSqliteScheduleStore());
    }

    [Fact]
    public async Task Registration_ResolvesTheStoreAsIScheduleStore()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddNymBroker().AddSqliteScheduleStore(Settings()).Build();
        await using var provider = services.BuildServiceProvider();

        Assert.Same(provider.GetRequiredService<SqliteScheduleStore>(), provider.GetRequiredService<IScheduleStore>());
    }

    [Fact]
    public void TableName_IsQuoted() => Assert.Equal("\"a\"\"b\"", SqliteScheduleSql.QuoteIdentifier("a\"b"));

    [Fact]
    public async Task InMemoryDatabase_Works()
    {
        await using var store = new SqliteScheduleStore(new SqliteScheduleStoreSettings { ConnectionString = "Data Source=:memory:" },
            NullLogger<SqliteScheduleStore>.Instance);
        Assert.True(await store.TryClaimAsync("job", DateTimeOffset.UnixEpoch.AddHours(1), Ct));
        Assert.Equal(DateTimeOffset.UnixEpoch.AddHours(1), await store.GetLastRunAsync("job", Ct));
    }
}
