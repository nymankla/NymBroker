using System.Text;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Factory;
using NymBroker.Core.Factory.Configuration;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Endpoint.Postgres;
using NymBroker.Endpoint.SqlServer;

namespace NymBroker.Tests;

public sealed class BuilderConfigurationTests
{
    // --- AddConsumer guard ---

    // Implements the marker IMessageConsumer but NOT the generic IConsume<T>,
    // so the runtime guard in AddConsumer should throw.
    private sealed class NoConsumeInterfaceConsumer : IMessageConsumer
    {
    }

    [Fact]
    public void AddConsumer_ThrowsInvalidOperationException_WhenTypeDoesNotImplementIConsumeT()
    {
        var services = new ServiceCollection();
        var builder = services.AddNymBroker();
        Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<NoConsumeInterfaceConsumer>());
    }

    // --- Build guard ---

    [Fact]
    public void Build_ThrowsInvalidOperationException_WhenCalledMoreThanOnce()
    {
        var services = new ServiceCollection();
        var builder = services.AddNymBroker();
        builder.Build();
        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    // --- AddMemoryEndPoint registers and resolves endpoint ---

    [Fact]
    public async Task AddMemoryEndPoint_EndpointIsReachableViaINymBroker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddMemoryEndPoint("Mem")
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        // PostAsync should succeed — endpoint is registered.
        await broker.PostAsync("Mem", new { Value = 42 }, TestContext.Current.CancellationToken);
    }

    // --- ApplyConfiguration for Memory and File types ---

    [Fact]
    public async Task ApplyConfiguration_Memory_RegistersEndpoint()
    {
        var config = new BrokerConfiguration
        {
            Endpoints =
            [
                new EndPointConfiguration { Name = "CfgMem", Type = EndPointType.Memory }
            ]
        };

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .ApplyConfiguration(config)
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.PostAsync("CfgMem", new { Ok = true }, TestContext.Current.CancellationToken);
    }

    // --- BrokerConfigurationReader from file ---

    [Fact]
    public void BrokerConfigurationReader_Read_FromFile_ReturnsParsedEndpoints()
    {
        var json = """
            {
              "NymBroker": {
                "Endpoints": [
                  { "name": "FileIn",  "type": "File",   "config": { "readPath": "in",  "postPath": "out" } },
                  { "name": "MemQ",    "type": "Memory"                                                     }
                ]
              }
            }
            """;

        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, json);
            var config = BrokerConfigurationReader.Read(path);

            Assert.Equal(2, config.Endpoints.Count);
            Assert.Equal("FileIn", config.Endpoints[0].Name);
            Assert.Equal(EndPointType.File, config.Endpoints[0].Type);
            Assert.Equal("MemQ", config.Endpoints[1].Name);
            Assert.Equal(EndPointType.Memory, config.Endpoints[1].Type);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void BrokerConfigurationReader_Read_MissingSectionKey_ReturnsEmptyConfig()
    {
        var json = """{ "OtherSection": {} }""";
        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, json);
            var config = BrokerConfigurationReader.Read(path);
            Assert.Empty(config.Endpoints);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void BrokerConfigurationReader_Read_FromIConfiguration_ReturnsParsedEndpoints()
    {
        var json = """
            {
              "NymBroker": {
                "Endpoints": [
                  { "name": "Mem1", "type": "Memory" },
                  { "name": "Pg1", "type": "Postgres" }
                ]
              }
            }
            """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var configuration = new ConfigurationBuilder()
            .AddJsonStream(stream)
            .Build();

        var config = BrokerConfigurationReader.Read(configuration);

        Assert.Equal(2, config.Endpoints.Count);
        Assert.Equal("Mem1", config.Endpoints[0].Name);
        Assert.Equal(EndPointType.Memory, config.Endpoints[0].Type);
        Assert.Equal("Pg1", config.Endpoints[1].Name);
        Assert.Equal(EndPointType.Postgres, config.Endpoints[1].Type);
    }

    [Fact]
    public void BrokerConfigurationReader_Read_FromIConfiguration_MissingSection_ReturnsEmpty()
    {
        var configuration = new ConfigurationBuilder().Build();
        var config = BrokerConfigurationReader.Read(configuration);
        Assert.Empty(config.Endpoints);
    }

    [Theory]
    [InlineData("\"WriteOnly\"")]
    [InlineData("\"writeonly\"")]
    [InlineData("2")]
    public void BrokerConfigurationReader_Read_Mode_AcceptsNameOrNumber(string mode)
    {
        var json = "{ \"NymBroker\": { \"Endpoints\": [ { \"Name\": \"Out\", \"Type\": \"Memory\", \"Mode\": " + mode + " } ] } }";
        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, json);
            var config = BrokerConfigurationReader.Read(path);
            Assert.Equal(EndpointMode.WriteOnly, config.Endpoints[0].Mode);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void BrokerConfigurationReader_Read_InvalidMode_Throws()
    {
        var json = """{ "NymBroker": { "Endpoints": [ { "Name": "Out", "Type": "Memory", "Mode": "Bogus" } ] } }""";
        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, json);
            Assert.Throws<System.Text.Json.JsonException>(() => BrokerConfigurationReader.Read(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    // --- Open endpoint types ---

    [Fact]
    public void BrokerConfigurationReader_Read_UnknownType_IsLoadedAsIs()
    {
        var json = """
            {
              "NymBroker": {
                "Endpoints": [
                  { "name": "Mem1",   "type": "Memory" },
                  { "name": "Custom", "type": "SqlServer", "config": { "tableName": "dbo.q" } }
                ]
              }
            }
            """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var config = BrokerConfigurationReader.Read(new ConfigurationBuilder().AddJsonStream(stream).Build());

        Assert.Equal(2, config.Endpoints.Count);
        Assert.Equal("SqlServer", config.Endpoints[1].Type);
        Assert.True(config.Endpoints[1].Config.HasValue);
    }

    [Fact]
    public void EndPointConfiguration_IsType_IsCaseInsensitive()
    {
        var ep = new EndPointConfiguration { Name = "M", Type = "memory" };

        Assert.True(ep.IsType(EndPointType.Memory));
        Assert.False(ep.IsType(EndPointType.File));
    }

    [Fact]
    public async Task ApplyConfiguration_UnknownType_IsLeftForExtensionToRegister()
    {
        var config = new BrokerConfiguration
        {
            Endpoints =
            [
                new EndPointConfiguration { Name = "CfgMem", Type = "memory" },
                new EndPointConfiguration { Name = "Ext",    Type = "Custom" }
            ]
        };

        var services = new ServiceCollection();
        services.AddLogging();
        var builder = services.AddNymBroker().ApplyConfiguration(config);

        // What a With*() extension in another package does for its own type name.
        foreach (var ep in config.Endpoints.Where(ep => ep.IsType("Custom")))
        {
            builder.Services.AddKeyedSingleton<IEndPoint>(ep.Name, (_, _) => new MemoryQueueEndPoint(ep.Name));
            builder.RegisterEndpoint(ep.Name);
        }
        builder.Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.PostAsync("CfgMem", new { Ok = true }, TestContext.Current.CancellationToken);
        await broker.PostAsync("Ext", new { Ok = true }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public void EndPointType_BuiltIn_ListsShippedTypes()
    {
        Assert.Equal(["File", "RabbitMq", "Memory", "Sql", "Postgres"], EndPointType.BuiltIn);
    }

    // --- SQL Server add-on ---

    [Fact]
    public async Task AddSqlServerEndPoint_RegistersEndpointInContainer()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddNymBroker()
            .AddSqlServerEndPoint("Mssql", new SqlServerSettings { AutoCreateTable = false })
            .Build();

        await using var sp = services.BuildServiceProvider();

        Assert.IsType<SqlServerEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Mssql"));
    }

    [Fact]
    public async Task WithSqlServer_RegistersConfiguredEndpoint_CaseInsensitively()
    {
        var json = """
            {
              "NymBroker": {
                "Endpoints": [
                  { "name": "Mem1",   "type": "Memory" },
                  { "name": "Mssql1", "type": "sqlserver", "config": { "tableName": "dbo.orders_queue", "autoCreateTable": false, "batchSize": 25 } }
                ]
              }
            }
            """;

        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, json);
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddNymBroker()
                .LoadConfiguration(path)
                .WithSqlServer()
                .Build();

            await using var sp = services.BuildServiceProvider();

            Assert.IsType<SqlServerEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Mssql1"));
            Assert.IsType<MemoryQueueEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Mem1"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task ApplyConfiguration_FromIConfiguration_IsSeenByWithExtensions()
    {
        // Before the fix only LoadConfiguration(file) set LoadedConfiguration, so With…() ignored these entries.
        var json = """
            {
              "NymBroker": {
                "Endpoints": [
                  { "name": "Pg1", "type": "Postgres", "config": { "tableName": "orders_queue", "autoCreateTable": false } }
                ]
              }
            }
            """;
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var configuration = new ConfigurationBuilder().AddJsonStream(stream).Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .ApplyConfiguration(BrokerConfigurationReader.Read(configuration))
            .WithPostgres()
            .Build();

        await using var sp = services.BuildServiceProvider();
        Assert.IsType<PostgresEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Pg1"));
    }

    public sealed class CustomSettings
    {
        public string Topic { get; set; } = "default-topic";
        public int Partitions { get; set; } = 1;
    }

    [Fact]
    public void AddConfiguredEndPoints_CallsRegisterForMatchingTypes_WithDeserializedSettings()
    {
        var config = new BrokerConfiguration
        {
            Endpoints =
            [
                new EndPointConfiguration { Name = "K1", Type = "kafka", Config = System.Text.Json.JsonDocument.Parse("""{ "topic": "orders", "partitions": 3 }""").RootElement },
                new EndPointConfiguration { Name = "K2", Type = "Kafka" },
                new EndPointConfiguration { Name = "Mem", Type = "Memory" }
            ]
        };

        var seen = new List<(string Name, CustomSettings Settings)>();
        new ServiceCollection().AddNymBroker()
            .ApplyConfiguration(config)
            .AddConfiguredEndPoints("Kafka", ep => seen.Add((ep.Name, ep.GetSettings<CustomSettings>())));

        Assert.Collection(seen,
            k1 => { Assert.Equal("K1", k1.Name); Assert.Equal("orders", k1.Settings.Topic); Assert.Equal(3, k1.Settings.Partitions); },
            k2 => { Assert.Equal("K2", k2.Name); Assert.Equal("default-topic", k2.Settings.Topic); });
    }

    [Fact]
    public void AddConfiguredEndPoints_WithoutLoadedConfiguration_DoesNothing()
    {
        var called = false;
        new ServiceCollection().AddNymBroker().AddConfiguredEndPoints("Kafka", _ => called = true);
        Assert.False(called);
    }

    // --- EndPointConfiguration.ToFileSettings ---

    [Fact]
    public void EndPointConfiguration_ToFileSettings_ReturnsDefaults_WhenConfigIsNull()
    {
        var ep = new EndPointConfiguration { Name = "F", Type = EndPointType.File };
        var settings = ep.ToFileSettings();
        Assert.NotNull(settings);
    }

    [Fact]
    public async Task AddPostgresEndPoint_RegistersEndpointInContainer()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddNymBroker()
            .AddPostgresEndPoint("Pg", new PostgresSettings { AutoCreateTable = false })
            .Build();

        await using var sp = services.BuildServiceProvider();
        var endpoint = sp.GetRequiredKeyedService<IEndPoint>("Pg");

        Assert.IsType<PostgresEndPoint>(endpoint);
    }

    // --- RegisterEndpoint adds name to the endpoint list ---

    [Fact]
    public void RegisterEndpoint_AddsExternalEndpointName()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var ep = new MemoryQueueEndPoint("External");
        services.AddKeyedSingleton<IEndPoint>("External", ep);

        var builder = services.AddNymBroker();
        builder.RegisterEndpoint("External");
        builder.Build();

        // Building with a registered external endpoint should not throw.
    }
}
