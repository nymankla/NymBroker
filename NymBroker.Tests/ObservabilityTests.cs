using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Collections.Immutable;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Resilience;

namespace NymBroker.Tests;

public sealed class ObservabilityTests
{
    private sealed class ObservedMessage
    {
        public string Value { get; set; } = "";
    }

    private sealed record CapturedMeasurement(string Instrument, long Value, Dictionary<string, object?> Tags);

    private sealed class MetricCapture : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly string _source;
        private readonly List<CapturedMeasurement> _measurements = [];

        public MetricCapture(string source)
        {
            _source = source;
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name == NymBrokerDiagnostics.InstrumentationName)
                        listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                if (!HasTag(tags, "source", _source)) return;
                var capturedTags = tags.ToArray().ToDictionary(tag => tag.Key, tag => tag.Value);
                lock (_measurements)
                    _measurements.Add(new CapturedMeasurement(instrument.Name, value, capturedTags));
            });
            _listener.Start();
        }

        public CapturedMeasurement[] For(string instrument)
        {
            lock (_measurements)
                return _measurements.Where(m => m.Instrument == instrument).ToArray();
        }

        public void Dispose() => _listener.Dispose();
    }

    private sealed class FailingEndPoint : IEndPoint
    {
        public Task PostAsync(byte[] message, CancellationToken ct = default)
            => throw new InvalidOperationException("Endpoint intentionally failed");

        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    private sealed class ObservedConsumer(bool fail = false, bool cancel = false) : IConsume<ObservedMessage>
    {
        public Task ConsumeAsync(ObservedMessage message, IMessageContext context, CancellationToken ct = default)
        {
            if (cancel) throw new OperationCanceledException();
            if (fail) throw new InvalidOperationException("Consumer intentionally failed");
            return Task.CompletedTask;
        }
    }

    private abstract class ObservedSubscriber(bool fail) : ISubscribe<ObservedMessage>
    {
        public Task ReceiveAsync(ObservedMessage message, IMessageContext context, CancellationToken ct = default)
        {
            if (fail) throw new InvalidOperationException("Subscriber intentionally failed");
            return Task.CompletedTask;
        }
    }

    private sealed class FirstSubscriber() : ObservedSubscriber(false) { }
    private sealed class FailingSubscriber() : ObservedSubscriber(true) { }
    private sealed class ThirdSubscriber() : ObservedSubscriber(false) { }

    [Fact]
    public async Task ProcessAsync_EmitsActivityWithMessageAndCorrelationIds()
    {
        var source = "observability-test-" + Guid.NewGuid();
        var serializer = new MessageSerializerJson();
        var context = new MessageContext<ObservedMessage>
        {
            Message = new ObservedMessage { Value = "test" },
            CorrelationId = Guid.NewGuid()
        };
        using var stream = serializer.Serialize(context);
        using var bytes = new MemoryStream();
        stream.CopyTo(bytes);

        using var services = new ServiceCollection().AddLogging().BuildServiceProvider();
        var broker = new NymBrokerImpl(
            serializer,
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(services.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(services.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);
        broker.AddEndpoint("Destination", new MemoryQueueEndPoint("Destination"));
        broker.Route<ObservedMessage>().To("Destination").Build();

        Activity? captured = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = activitySource => activitySource.Name == NymBrokerActivitySource.InstrumentationName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            // The listener is process-wide; ignore activities from tests running in parallel.
            ActivityStopped = activity =>
            {
                if ((string?)activity.GetTagItem("nymbroker.source") == source)
                    captured = activity;
            }
        };
        ActivitySource.AddActivityListener(listener);

        await broker.ProcessAsync(bytes.ToArray(), source, TestContext.Current.CancellationToken);

        Assert.NotNull(captured);
        Assert.Equal(context.Id, captured!.GetTagItem("messaging.message.id"));
        Assert.Equal(context.CorrelationId, captured.GetTagItem("messaging.conversation_id"));
        Assert.Equal(source, captured.GetTagItem("nymbroker.source"));
    }

    [Fact]
    public async Task ProcessAsync_RecordsReceivedFailedAndDurationMetrics()
    {
        var source = "observability-metrics-test-" + Guid.NewGuid();
        long received = 0;
        long failed = 0;
        var durations = 0;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == NymBrokerDiagnostics.InstrumentationName)
                    meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
        {
            if (HasTag(tags, "source", source))
            {
                if (instrument.Name == "nymbroker.messages.received") received += measurement;
                if (instrument.Name == "nymbroker.messages.failed") failed += measurement;
            }
        });
        listener.SetMeasurementEventCallback<double>((instrument, _, tags, _) =>
        {
            if (instrument.Name == "nymbroker.message.processing.duration"
                && HasTag(tags, "source", source))
                durations++;
        });
        listener.Start();

        using var services = new ServiceCollection().AddLogging().BuildServiceProvider();
        var serializer = new MessageSerializerJson();
        var broker = new NymBrokerImpl(
            serializer,
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(services.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(services.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        await broker.ProcessAsync("invalid-json", source, TestContext.Current.CancellationToken);

        Assert.Equal(1, received);
        Assert.Equal(1, failed);
        Assert.Equal(1, durations);
    }

    [Fact]
    public async Task RetryPolicy_RecordsRetryAttempts()
    {
        long retries = 0;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == RetryPolicy.InstrumentationName)
                    meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
        {
            if (instrument.Name == "nymbroker.retries") retries += measurement;
        });
        listener.Start();

        var attempt = 0;
        var policy = new RetryPolicy(new RetryOptions { Delay = TimeSpan.Zero });
        await policy.ExecuteAsync(_ =>
        {
            if (attempt++ == 0) throw new IOException("transient");
            return ValueTask.CompletedTask;
        }, TestContext.Current.CancellationToken);

        Assert.True(retries >= 1);
    }

    [Fact]
    public async Task ProcessAsync_RecordsEachSuccessfulRouteDelivery()
    {
        var source = "observability-route-" + Guid.NewGuid();
        using var capture = new MetricCapture(source);
        using var services = CreateServices();
        var broker = CreateBroker(services);
        broker.AddEndpoint("Destination1", new MemoryQueueEndPoint("Destination1"));
        broker.AddEndpoint("Destination2", new MemoryQueueEndPoint("Destination2"));
        broker.Route<ObservedMessage>().To("Destination1").Build();
        broker.Route<ObservedMessage>().To("Destination2").Build();

        await broker.ProcessAsync(await SerializeAsync(new ObservedMessage()), source, TestContext.Current.CancellationToken);

        var routed = capture.For("nymbroker.messages.routed");
        Assert.Equal(2, routed.Length);
        Assert.Equal(new[] { "Destination1", "Destination2" },
            routed.Select(m => (string)m.Tags["destination"]!).OrderBy(d => d));
        Assert.All(routed, measurement =>
        {
            Assert.Equal(source, measurement.Tags["source"]);
            Assert.Equal(MessageTypeName.Get(typeof(ObservedMessage)), measurement.Tags["message_type"]);
            Assert.Equal("route", measurement.Tags["via"]);
            Assert.Equal("success", measurement.Tags["outcome"]);
        });
    }

    [Fact]
    public async Task ProcessAsync_RecordsFailedRouteDelivery()
    {
        var source = "observability-route-failure-" + Guid.NewGuid();
        using var capture = new MetricCapture(source);
        using var services = CreateServices();
        var broker = CreateBroker(services);
        broker.AddEndpoint("FailingDestination", new FailingEndPoint());
        broker.Route<ObservedMessage>().To("FailingDestination").Build();

        await broker.ProcessAsync(await SerializeAsync(new ObservedMessage()), source, TestContext.Current.CancellationToken);

        var routed = Assert.Single(capture.For("nymbroker.messages.routed"));
        Assert.Equal("FailingDestination", routed.Tags["destination"]);
        Assert.Equal("failure", routed.Tags["outcome"]);
    }

    [Fact]
    public async Task ProcessAsync_RecordsTopicEndpointDelivery()
    {
        var source = "observability-topic-route-" + Guid.NewGuid();
        using var capture = new MetricCapture(source);
        using var services = CreateServices();
        var broker = CreateBroker(services);
        broker.AddEndpoint("TopicDestination", new MemoryQueueEndPoint("TopicDestination"));
        broker.AddTopic(new TopicContext
        {
            TopicName = "observed-topic",
            MessageType = typeof(ObservedMessage),
            SubscriberEndpoints = ImmutableList.Create("TopicDestination")
        });

        await broker.ProcessAsync(await SerializeAsync(new ObservedMessage()), source, TestContext.Current.CancellationToken);

        var routed = Assert.Single(capture.For("nymbroker.messages.routed"));
        Assert.Equal("TopicDestination", routed.Tags["destination"]);
        Assert.Equal("topic", routed.Tags["via"]);
        Assert.Equal("observed-topic", routed.Tags["topic"]);
        Assert.Equal("success", routed.Tags["outcome"]);
    }

    [Fact]
    public async Task ProcessAsync_RecordsConsumerSuccessAndFailure()
    {
        foreach (var fail in new[] { false, true })
        {
            var source = "observability-consumer-" + Guid.NewGuid();
            using var capture = new MetricCapture(source);
            var consumer = new ObservedConsumer(fail);
            using var services = CreateServices(collection =>
                collection.AddKeyedSingleton<IMessageConsumer>(nameof(ObservedConsumer), consumer));
            var broker = CreateBroker(services);
            broker.RegisterConsumer(typeof(ObservedMessage), nameof(ObservedConsumer));

            await broker.ProcessAsync(await SerializeAsync(new ObservedMessage()), source, TestContext.Current.CancellationToken);

            var consumed = Assert.Single(capture.For("nymbroker.messages.consumed"));
            Assert.Equal(source, consumed.Tags["source"]);
            Assert.Equal(MessageTypeName.Get(typeof(ObservedMessage)), consumed.Tags["message_type"]);
            Assert.Equal(nameof(ObservedConsumer), consumed.Tags["consumer"]);
            Assert.Equal("consumer", consumed.Tags["kind"]);
            Assert.Equal(fail ? "failure" : "success", consumed.Tags["outcome"]);
        }
    }

    [Fact]
    public async Task ProcessAsync_RecordsEachTopicSubscriberOutcome()
    {
        var source = "observability-subscriber-" + Guid.NewGuid();
        using var capture = new MetricCapture(source);
        using var services = CreateServices(collection =>
        {
            collection.AddKeyedSingleton<IMessageSubscriber>(nameof(FirstSubscriber), new FirstSubscriber());
            collection.AddKeyedSingleton<IMessageSubscriber>(nameof(FailingSubscriber), new FailingSubscriber());
            collection.AddKeyedSingleton<IMessageSubscriber>(nameof(ThirdSubscriber), new ThirdSubscriber());
        });
        var broker = CreateBroker(services);
        broker.AddTopic(new TopicContext
        {
            TopicName = "observed-subscribers",
            MessageType = typeof(ObservedMessage),
            SubscriberDispatchers = ImmutableList.Create<(Type, string)>(
                (typeof(FirstSubscriber), nameof(FirstSubscriber)),
                (typeof(FailingSubscriber), nameof(FailingSubscriber)),
                (typeof(ThirdSubscriber), nameof(ThirdSubscriber)))
        });

        await broker.ProcessAsync(await SerializeAsync(new ObservedMessage()), source, TestContext.Current.CancellationToken);

        var consumed = capture.For("nymbroker.messages.consumed");
        Assert.Equal(3, consumed.Length);
        Assert.Equal(2, consumed.Count(m => Equals(m.Tags["outcome"], "success")));
        Assert.Equal(1, consumed.Count(m => Equals(m.Tags["outcome"], "failure")));
        Assert.All(consumed, measurement =>
        {
            Assert.Equal("subscriber", measurement.Tags["kind"]);
            Assert.Equal(MessageTypeName.Get(typeof(ObservedMessage)), measurement.Tags["message_type"]);
        });
    }

    [Fact]
    public async Task ProcessAsync_DoesNotRecordConsumedWithoutConsumerOrOnCancellation()
    {
        var noConsumerSource = "observability-no-consumer-" + Guid.NewGuid();
        using var noConsumerCapture = new MetricCapture(noConsumerSource);
        using var emptyServices = CreateServices();
        var noConsumerBroker = CreateBroker(emptyServices);
        await noConsumerBroker.ProcessAsync(await SerializeAsync(new ObservedMessage()), noConsumerSource, TestContext.Current.CancellationToken);
        Assert.Empty(noConsumerCapture.For("nymbroker.messages.consumed"));

        var cancellationSource = "observability-cancelled-consumer-" + Guid.NewGuid();
        using var cancellationCapture = new MetricCapture(cancellationSource);
        var consumer = new ObservedConsumer(cancel: true);
        using var consumerServices = CreateServices(collection =>
            collection.AddKeyedSingleton<IMessageConsumer>(nameof(ObservedConsumer), consumer));
        var cancellationBroker = CreateBroker(consumerServices);
        cancellationBroker.RegisterConsumer(typeof(ObservedMessage), nameof(ObservedConsumer));

        var raw = await SerializeAsync(new ObservedMessage());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            cancellationBroker.ProcessAsync(raw, cancellationSource, TestContext.Current.CancellationToken));

        Assert.Empty(cancellationCapture.For("nymbroker.messages.consumed"));
    }

    private static ServiceProvider CreateServices(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private static NymBrokerImpl CreateBroker(IServiceProvider services)
    {
        var serializer = new MessageSerializerJson();
        return new NymBrokerImpl(
            serializer,
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(services.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(services.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);
    }

    private static async Task<string> SerializeAsync<T>(T message) where T : class
    {
        var serializer = new MessageSerializerJson();
        using var stream = serializer.Serialize(new MessageContext<T> { Message = message });
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    private static bool HasTag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key, object? value)
    {
        foreach (var tag in tags)
            if (tag.Key == key && Equals(tag.Value, value))
                return true;
        return false;
    }
}
