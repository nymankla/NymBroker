using System.Text;
using MessageBroker.Core.Consume;
using MessageBroker.Core.DI;
using MessageBroker.Core.Endpoint;
using MessageBroker.Core.Endpoint.HealthCheck;
using MessageBroker.Core.Endpoint.Memory;
using MessageBroker.Core.Factory;
using MessageBroker.Core.Factory.Configuration;
using MessageBroker.Core.Impl;
using MessageBroker.Core.Message;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MessageBroker.Tests;

public sealed class BuilderConfigurationTests
{
    // --- AddConsumer guard ---

    // Implements the marker IMessageConsumer but NOT the generic IConsume<T>,
    // so the runtime guard in AddConsumer should throw.
    private sealed class NoConsumeInterfaceConsumer : IMessageConsumer
    {
        public string Name => nameof(NoConsumeInterfaceConsumer);
    }

    [Fact]
    public void AddConsumer_ThrowsInvalidOperationException_WhenTypeDoesNotImplementIConsumeT()
    {
        var services = new ServiceCollection();
        var builder = services.AddMessageBroker();
        Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<NoConsumeInterfaceConsumer>());
    }

    // --- Build guard ---

    [Fact]
    public void Build_ThrowsInvalidOperationException_WhenCalledMoreThanOnce()
    {
        var services = new ServiceCollection();
        var builder = services.AddMessageBroker();
        builder.Build();
        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    // --- AddMemoryEndPoint registers and resolves endpoint ---

    [Fact]
    public async Task AddMemoryEndPoint_EndpointIsReachableViaIMessageBroker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMessageBroker()
            .AddMemoryEndPoint("Mem")
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<IMessageBroker>();

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
        services.AddMessageBroker()
            .ApplyConfiguration(config)
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<IMessageBroker>();

        await broker.PostAsync("CfgMem", new { Ok = true });
    }

    // --- BrokerConfigurationReader from file ---

    [Fact]
    public void BrokerConfigurationReader_Read_FromFile_ReturnsParsedEndpoints()
    {
        var json = """
            {
              "MessageBroker": {
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
              "MessageBroker": {
                "Endpoints": [
                  { "name": "Mem1", "type": "Memory" }
                ]
              }
            }
            """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var configuration = new ConfigurationBuilder()
            .AddJsonStream(stream)
            .Build();

        var config = BrokerConfigurationReader.Read(configuration);

        Assert.Single(config.Endpoints);
        Assert.Equal("Mem1", config.Endpoints[0].Name);
        Assert.Equal(EndPointType.Memory, config.Endpoints[0].Type);
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

    // --- RegisterEndpoint adds name to the endpoint list ---

    [Fact]
    public void RegisterEndpoint_AddsExternalEndpointName()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var ep = new MemoryQueueEndPoint("External");
        services.AddKeyedSingleton<IEndPoint>("External", ep);

        var builder = services.AddMessageBroker();
        builder.RegisterEndpoint("External");
        builder.Build();

        // Building with a registered external endpoint should not throw.
    }
}
