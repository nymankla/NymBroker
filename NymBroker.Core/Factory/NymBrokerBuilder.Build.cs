using System.Collections.Immutable;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Factory.Configuration;
using NymBroker.Core.DI;
using NymBroker.Core.Filter;
using NymBroker.Core.Idempotency;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Route;
using NymBroker.Core.Scheduling;
using NymBroker.Core.Serialize;
using NymBroker.Core.Splitter;
using NymBroker.Core.Transform;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Factory;

public sealed partial class NymBrokerBuilder
{
    public void Build()
    {
        if (_built)
            throw new InvalidOperationException("NymBrokerBuilder.Build() can only be called once.");

        ValidateHealthCheckEndpoints();
        _built = true;

        RegisterCoreServices(_services, _jsonContexts);

        // Snapshot the builder state now: the factory runs later, when the container first resolves the broker.
        _services.AddSingleton<NymBrokerImpl>(new BrokerBlueprint(this).CreateBroker);

        _services.AddSingleton<INymBroker>(sp => sp.GetRequiredService<NymBrokerImpl>());
        _services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, NymBrokerHostedService>());

        // After the broker's hosted service: the host starts them in order and stops them in reverse.
        if (_outbox)
            _services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, Outbox.OutboxDispatcher>());
    }

    private void ValidateHealthCheckEndpoints()
    {
        var unknownNonCritical = _healthCheckOptions.NonCriticalEndpoints
            .Where(n => !_endpoints.Contains(n, StringComparer.OrdinalIgnoreCase))
            .ToList();
        if (unknownNonCritical.Count > 0)
            throw new InvalidOperationException(
                $"ConfigureHealthCheck marks unknown endpoint(s) as non-critical: {string.Join(", ", unknownNonCritical)}. " +
                "Register the endpoint, or remove it from NonCritical(...).");
    }

    private static void RegisterCoreServices(IServiceCollection services, IEnumerable<System.Text.Json.Serialization.JsonSerializerContext> jsonContexts)
    {
        services.AddSingleton(new MessageSerializerJson(jsonContexts));
        services.AddSingleton<IMessageSerializer>(sp => sp.GetRequiredService<MessageSerializerJson>());
        services.AddSingleton<IAggregator, AggregatorImpl>();
        services.AddSingleton<ISplitter, SplitterImpl>();
        services.AddSingleton<ICompressor, BrotliCompressor>();
        services.AddSingleton<MessageTypeRegistry>();
        services.AddSingleton<ConsumerDispatcher>();
        services.AddSingleton<SubscriberDispatcher>();
        services.TryAddSingleton<IInbox, Inbox>();
    }

    /// <summary>
    /// The builder's state copied at <see cref="Build"/>, applied to the broker when the container creates it.
    /// The order in <see cref="CreateBroker"/> matters, e.g. the schedule store is set before the scheduled actions are added.
    /// </summary>
    private sealed class BrokerBlueprint(NymBrokerBuilder builder)
    {
        private readonly List<string> _endpoints = builder._endpoints.ToList();
        private readonly List<(Type ConsumerType, Type MessageType, bool RetryOnFailure, Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>? Dispatcher)> _consumers = builder._consumers.ToList();
        private readonly List<TopicContext> _topicContexts = builder._topicContexts.ToList();
        private readonly List<TopicConfiguration> _configTopics = builder._configTopics.ToList();
        private readonly List<(Type TransformerType, string? Endpoint)> _transformers = builder._transformers.ToList();
        private readonly List<(Type TransformerType, string Endpoint)> _outputTransformers = builder._outputTransformers.ToList();
        private readonly List<Type> _builderFilterTypes = builder._builderFilterTypes.ToList();
        private readonly List<IMessageFilter> _filterInstances = builder._filterInstances.ToList();
        private readonly List<RouteContext> _routes = builder._routes.ToList();
        private readonly List<Action<NymBrokerImpl, IServiceProvider>> _scheduledActions = builder._scheduledActions.ToList();
        private readonly List<string> _wireTapEndpoints = builder._wireTapEndpoints.ToList();
        private readonly string? _deadLetterEndpoint = builder._deadLetterEndpoint;
        private readonly TimeSpan? _maxMessageAge = builder._maxMessageAge;
        private readonly bool _idempotentReceiver = builder._idempotentReceiver;
        private readonly bool _scheduleStore = builder._scheduleStore;
        private readonly BrokerHealthCheckOptions _healthCheckOptions = builder._healthCheckOptions;

        public NymBrokerImpl CreateBroker(IServiceProvider sp)
        {
            var broker = new NymBrokerImpl(
                sp.GetRequiredService<MessageSerializerJson>(),
                sp.GetRequiredService<IAggregator>(),
                sp.GetRequiredService<MessageTypeRegistry>(),
                sp.GetRequiredService<ConsumerDispatcher>(),
                sp.GetRequiredService<SubscriberDispatcher>(),
                sp.GetRequiredService<ILogger<NymBrokerImpl>>(),
                sp.GetRequiredService<ISplitter>(),
                sp.GetRequiredService<ICompressor>());

            AddEndpointsAndConsumers(broker, sp);
            AddPipeline(broker, sp);
            AddScheduling(broker, sp);
            AddPolicies(broker, sp);
            AddConfigTopics(broker, sp.GetRequiredService<MessageTypeRegistry>());
            return broker;
        }

        private void AddEndpointsAndConsumers(NymBrokerImpl broker, IServiceProvider sp)
        {
            foreach (var endpointName in _endpoints)
                broker.AddEndpoint(endpointName, sp.GetRequiredKeyedService<IEndPoint>(endpointName));

            foreach (var (consumerType, messageType, retryOnFailure, dispatcher) in _consumers)
                broker.RegisterConsumer(messageType, consumerType.Name, retryOnFailure, dispatcher);

            foreach (var topic in _topicContexts)
                broker.AddTopic(topic);
        }

        private void AddPipeline(NymBrokerImpl broker, IServiceProvider sp)
        {
            foreach (var (type, endpoint) in _transformers)
                broker.AddInputTransformer((IInputTransformer)sp.GetRequiredService(type), endpoint);

            foreach (var (type, endpoint) in _outputTransformers)
                broker.AddOutputTransformer((IOutputTransformer)sp.GetRequiredService(type), endpoint);

            foreach (var filterType in _builderFilterTypes)
                broker.AddFilter((IMessageFilter)sp.GetRequiredService(filterType));

            foreach (var filter in _filterInstances)
                broker.AddFilter(filter);

            foreach (var route in _routes)
                broker.AddRoute(route);
        }

        private void AddScheduling(NymBrokerImpl broker, IServiceProvider sp)
        {
            // The store first: scheduled actions use it when they are added.
            if (_scheduleStore)
                broker.SetScheduleStore(sp.GetRequiredService<IScheduleStore>());

            foreach (var schedule in _scheduledActions)
                schedule(broker, sp);
        }

        private void AddPolicies(NymBrokerImpl broker, IServiceProvider sp)
        {
            foreach (var tap in _wireTapEndpoints)
                broker.AddWireTap(tap);

            if (_deadLetterEndpoint != null)
                broker.SetDeadLetterEndpoint(_deadLetterEndpoint);

            if (_maxMessageAge.HasValue)
                broker.SetMaxMessageAge(_maxMessageAge.Value);

            if (_idempotentReceiver)
                broker.SetIdempotencyStore(sp.GetRequiredService<IIdempotencyStore>());

            broker.ConfigureHealthCheck(_healthCheckOptions);
        }

        /// <summary>Config-based topics: resolves each message type string to a CLR type through the registry.</summary>
        private void AddConfigTopics(NymBrokerImpl broker, MessageTypeRegistry registry)
        {
            foreach (var topic in _configTopics)
            {
                var msgType = topic.MessageType != null
                    ? registry.Resolve(topic.MessageType) ?? typeof(Message.IAnyMessage)
                    : typeof(Message.IAnyMessage);
                broker.AddTopic(new TopicContext
                {
                    TopicName = topic.TopicName,
                    MessageType = msgType,
                    SubscriberEndpoints = topic.SubscriberEndpoints.ToImmutableList()
                });
            }
        }
    }
}
