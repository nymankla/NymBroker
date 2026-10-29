using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.DI;
using NymBroker.Core.Factory;
using NymBroker.Core.Scheduling;
using NymBroker.Scheduling.SqlServer;

namespace NymBroker.Tests;

/// <summary>
/// SQL Server schedule store. Settings and registration tests always run; the contract tests need
/// NYMBROKER_SQLSERVER_CS set (see <see cref="SqlServerEndPointTests"/>).
/// </summary>
public sealed class SqlServerScheduleStoreTests : ScheduleStoreContractTests
{
    private const string ConnectionStringVariable = "NYMBROKER_SQLSERVER_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);

    private SqlServerScheduleStoreSettings Settings() => new() { ConnectionString = ConnectionString!, TableName = $"dbo.{TableName}" };

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString), $"Set {ConnectionStringVariable} to run SQL Server integration tests.");

    protected override IScheduleStore CreateStore() => new SqlServerScheduleStore(Settings(), NullLogger<SqlServerScheduleStore>.Instance);

    protected override NymBrokerBuilder AddStore(NymBrokerBuilder builder) => builder.AddSqlServerScheduleStore(Settings());

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"DROP TABLE IF EXISTS dbo.{TableName}", connection);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public void InvalidSettings_Throw_AlsoAtRegistration()
    {
        Assert.Throws<ArgumentException>(new SqlServerScheduleStoreSettings { ConnectionString = " " }.Validate);
        Assert.Throws<ArgumentException>(new SqlServerScheduleStoreSettings { TableName = "dbo." }.Validate);
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddNymBroker()
            .AddSqlServerScheduleStore(new SqlServerScheduleStoreSettings { TableName = "" }));
    }

    [Fact]
    public async Task Registration_ResolvesTheStoreAsIScheduleStore()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddNymBroker().AddSqlServerScheduleStore().Build();
        await using var provider = services.BuildServiceProvider();

        Assert.Same(provider.GetRequiredService<SqlServerScheduleStore>(), provider.GetRequiredService<IScheduleStore>());
    }

    [Fact]
    public void TableName_IsQuoted() => Assert.Equal("[dbo].[a]]b]", SqlServerScheduleSql.QuoteQualifiedIdentifier("dbo.a]b"));
}
