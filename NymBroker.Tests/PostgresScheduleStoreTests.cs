using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NymBroker.Core.DI;
using NymBroker.Core.Factory;
using NymBroker.Core.Scheduling;
using NymBroker.Scheduling.Postgres;

namespace NymBroker.Tests;

/// <summary>
/// PostgreSQL schedule store. Settings and registration tests always run; the contract tests need
/// NYMBROKER_POSTGRES_CS set (see <see cref="PostgresEndPointTests"/>).
/// </summary>
public sealed class PostgresScheduleStoreTests : ScheduleStoreContractTests
{
    private const string ConnectionStringVariable = "NYMBROKER_POSTGRES_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);

    private PostgresScheduleStoreSettings Settings() => new() { ConnectionString = ConnectionString!, TableName = TableName };

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString), $"Set {ConnectionStringVariable} to run PostgreSQL integration tests.");

    protected override IScheduleStore CreateStore() => new PostgresScheduleStore(Settings(), NullLogger<PostgresScheduleStore>.Instance);

    protected override NymBrokerBuilder AddStore(NymBrokerBuilder builder) => builder.AddPostgresScheduleStore(Settings());

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"DROP TABLE IF EXISTS {PostgresScheduleSql.QuoteQualifiedIdentifier(TableName)}", connection);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public void InvalidSettings_Throw_AlsoAtRegistration()
    {
        Assert.Throws<ArgumentException>(new PostgresScheduleStoreSettings().Validate);
        Assert.Throws<ArgumentException>(new PostgresScheduleStoreSettings { ConnectionString = "Host=x", TableName = "public." }.Validate);
        Assert.Throws<ArgumentException>(new PostgresScheduleStoreSettings { ConnectionString = "Host=x", TableName = "a.b.c" }.Validate);
        Assert.Throws<ArgumentException>(new PostgresScheduleStoreSettings { ConnectionString = "Host=x", TableName = new string('t', 64) }.Validate);
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddNymBroker().AddPostgresScheduleStore());
    }

    [Fact]
    public async Task Registration_ResolvesTheStoreAsIScheduleStore()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddNymBroker().AddPostgresScheduleStore(new PostgresScheduleStoreSettings { ConnectionString = "Host=localhost" }).Build();
        await using var provider = services.BuildServiceProvider();

        Assert.Same(provider.GetRequiredService<PostgresScheduleStore>(), provider.GetRequiredService<IScheduleStore>());
    }

    [Fact]
    public void TableName_IsQuoted() => Assert.Equal("\"my\"\"schema\".\"t\"", PostgresScheduleSql.QuoteQualifiedIdentifier("my\"schema.t"));
}
