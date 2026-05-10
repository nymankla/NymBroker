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
using NymBroker.Postgres;

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
        await broker.PostAsync("Mem", new { Value = 42 });
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

        await broker.PostAsync("CfgMem", new { Ok = true });
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
