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
[Collection("SQL Server integration")]
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
    public async Task AutoCreate_StoresStartingAtOnce_AllClaim()
    {
        RequireDatabase();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var occurrence = DateTimeOffset.UtcNow;
        var tasks = Enumerable.Range(0, 16).Select(async i =>
        {
            var settings = Settings();
            settings.TableName = i % 2 == 0 ? $"dbo.{TableName}" : TableName;
            var store = new SqlServerScheduleStore(settings, NullLogger<SqlServerScheduleStore>.Instance);
            await gate.Task;
            return await store.TryClaimAsync($"job-{i}", occurrence, Ct);
        }).ToArray();
        gate.SetResult();

        Assert.All(await Task.WhenAll(tasks), Assert.True);
    }

    [Fact]
    public async Task AutoCreate_ConflictingConstraint_PropagatesCreationError()
    {
        RequireDatabase();
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        var otherTable = $"other_{TableName}";
        await using var create = new SqlCommand($"CREATE TABLE dbo.[{otherTable}] (id int NOT NULL CONSTRAINT [pk_dbo_{TableName}] PRIMARY KEY)", connection);
        await create.ExecuteNonQueryAsync(Ct);
        try
        {
            var store = new SqlServerScheduleStore(Settings(), NullLogger<SqlServerScheduleStore>.Instance);
            var error = await Assert.ThrowsAsync<SqlException>(() => store.GetLastRunAsync("job", Ct).AsTask());
            Assert.Equal(2714, error.Number);
        }
        finally
        {
            await using var drop = new SqlCommand($"DROP TABLE dbo.[{otherTable}]", connection);
            await drop.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    [Fact]
    public void TableName_IsQuoted() => Assert.Equal("[dbo].[a]]b]", SqlServerScheduleSql.QuoteQualifiedIdentifier("dbo.a]b"));
}
