using MessageBroker.Core.Aggregator;
using MessageBroker.Core.Consume;
using MessageBroker.Core.Endpoint;
using MessageBroker.Core.Endpoint.File;
using MessageBroker.Core.Endpoint.Memory;
using MessageBroker.Core.Factory.Configuration;
using MessageBroker.Core.DI;
using MessageBroker.Core.Impl;
using MessageBroker.Core.Serialize;
using MessageBroker.Core.Splitter;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Core.Factory;

public sealed class MessageBrokerBuilder
{
    private readonly IServiceCollection _services;
    private readonly List<string> _endpoints = [];
    private readonly List<(Type ConsumerType, Type MessageType)> _consumers = [];
    private bool _built;

    /// <summary>Exposes the DI container for endpoint extension packages (e.g. MessageBroker.RabbitMq).</summary>
    public IServiceCollection Services => _services;

    /// <summary>Set by <see cref="LoadConfiguration"/> so extension packages can process their endpoint types.</summary>
    public BrokerConfiguration? LoadedConfiguration { get; private set; }

    public MessageBrokerBuilder(IServiceCollection services) => _services = services;

    // --- Endpoint registration ---

    public MessageBrokerBuilder AddFileEndPoint(string name, FileSettings? settings = null)
    {
        var s = settings ?? new FileSettings();
        _services.AddKeyedSingleton<IEndPoint>(name, (sp, _) => new FileEndPoint(name, s, sp.GetRequiredService<ILogger<FileEndPoint>>()));
        _endpoints.Add(name);
        return this;
    }

    public MessageBrokerBuilder AddMemoryEndPoint(string name, int capacity = 1000)
    {
        _services.AddKeyedSingleton<IEndPoint>(name, (_, _) => new MemoryQueueEndPoint(name, capacity));
        _endpoints.Add(name);
        return this;
    }

    /// <summary>Registers an endpoint name that was added externally (e.g. by an extension package).</summary>
    public void RegisterEndpoint(string name) => _endpoints.Add(name);

    // --- Consumer registration ---

    public MessageBrokerBuilder AddConsumer<TConsumer>() where TConsumer : class, IMessageConsumer
    {
        var messageTypes = typeof(TConsumer).GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsume<>))
            .Select(i => i.GetGenericArguments()[0])
            .Distinct()
            .ToList();

        if (messageTypes.Count == 0)
            throw new InvalidOperationException($"{typeof(TConsumer).Name} must implement IConsume<T>.");

        foreach (var messageType in messageTypes)
            _consumers.Add((typeof(TConsumer), messageType));

        _services.AddKeyedTransient(typeof(IMessageConsumer), typeof(TConsumer).Name, typeof(TConsumer));
        return this;
    }

    // --- Load from config file ---

    public MessageBrokerBuilder LoadConfiguration(string filePath)
    {
        LoadedConfiguration = BrokerConfigurationReader.Read(filePath);
        return ApplyConfiguration(LoadedConfiguration);
    }

    public MessageBrokerBuilder ApplyConfiguration(BrokerConfiguration config)
    {
        foreach (var ep in config.Endpoints)
        {
            switch (ep.Type)
            {
                case EndPointType.File: AddFileEndPoint(ep.Name, ep.ToFileSettings()); break;
                case EndPointType.Memory: AddMemoryEndPoint(ep.Name); break;
                // EndPointType.RabbitMq is handled by MessageBroker.RabbitMq via WithRabbitMq()
            }
        }
        return this;
    }

    // --- Build ---

    public void Build()
    {
        if (_built)
            throw new InvalidOperationException("MessageBrokerBuilder.Build() can only be called once.");

        _built = true;

        _services.AddSingleton<MessageSerializerJson>();
        _services.AddSingleton<IMessageSerializer>(sp => sp.GetRequiredService<MessageSerializerJson>());
        _services.AddSingleton<IAggregator, AggregatorImpl>();
        _services.AddSingleton<ISplitter, SplitterImpl>();
        _services.AddSingleton<MessageTypeRegistry>();
        _services.AddSingleton<ConsumerDispatcher>();

        // Capture lists for closure.
        var endpoints = _endpoints.ToList();
        var consumers = _consumers.ToList();

        _services.AddSingleton<MessageBrokerImpl>(sp =>
        {
            var broker = new MessageBrokerImpl(
                sp.GetRequiredService<MessageSerializerJson>(),
                sp.GetRequiredService<IAggregator>(),
                sp.GetRequiredService<MessageTypeRegistry>(),
                sp.GetRequiredService<ConsumerDispatcher>(),
                sp.GetRequiredService<ILogger<MessageBrokerImpl>>());

            foreach (var endpointName in endpoints)
                broker.AddEndpoint(sp.GetRequiredKeyedService<IEndPoint>(endpointName));

            foreach (var (consumerType, messageType) in consumers)
                broker.RegisterConsumer(messageType, consumerType.Name);

            return broker;
        });

        _services.AddSingleton<IMessageBroker>(sp => sp.GetRequiredService<MessageBrokerImpl>());
        _services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, MessageBrokerHostedService>());
    }
}
