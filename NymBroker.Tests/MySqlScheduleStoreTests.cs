using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using MySqlConnector;
using NymBroker.Core.DI;
using NymBroker.Core.Factory;
using NymBroker.Core.Scheduling;
using NymBroker.Scheduling.MySql;

namespace NymBroker.Tests;

/// <summary>
/// MySQL schedule store. Settings and registration tests always run; the contract tests need
/// NYMBROKER_MYSQL_CS set (see <see cref="MySqlEndPointTests"/>).
/// </summary>
public sealed class MySqlScheduleStoreTests : ScheduleStoreContractTests
{
    private const string ConnectionStringVariable = "NYMBROKER_MYSQL_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);

    private MySqlScheduleStoreSettings Settings() => new() { ConnectionString = ConnectionString!, TableName = TableName };

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString), $"Set {ConnectionStringVariable} to run MySQL integration tests.");

    protected override IScheduleStore CreateStore() => new MySqlScheduleStore(Settings(), NullLogger<MySqlScheduleStore>.Instance);

    protected override NymBrokerBuilder AddStore(NymBrokerBuilder builder) => builder.AddMySqlScheduleStore(Settings());

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new MySqlCommand($"DROP TABLE IF EXISTS {TableName}", connection);
        await command.ExecuteNonQueryAsync();
    }

    [Theory]
    [InlineData("")]
    [InlineData("a.b.c")]
    [InlineData("x; DROP TABLE y")]
    [InlineData("`t`")]
    public void InvalidTableName_Throws(string tableName)
        => Assert.Throws<ArgumentException>(new MySqlScheduleStoreSettings { TableName = tableName }.Validate);

    [Fact]
    public void InvalidSettings_ThrowAtRegistration()
    {
        Assert.Throws<ArgumentException>(new MySqlScheduleStoreSettings { ConnectionString = " " }.Validate);
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddNymBroker()
            .AddMySqlScheduleStore(new MySqlScheduleStoreSettings { TableName = "bad name" }));
    }

    [Fact]
    public async Task Registration_ResolvesTheStoreAsIScheduleStore()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddNymBroker().AddMySqlScheduleStore().Build();
        await using var provider = services.BuildServiceProvider();

        Assert.Same(provider.GetRequiredService<MySqlScheduleStore>(), provider.GetRequiredService<IScheduleStore>());
    }

    [Fact]
    public void TableName_IsQuoted() => Assert.Equal("`db`.`t`", MySqlScheduleSql.QuoteTableName("db.t"));
}
