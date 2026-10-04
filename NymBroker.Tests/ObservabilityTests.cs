using System.Diagnostics;
using System.Diagnostics.Metrics;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Resilience;

namespace NymBroker.Tests;

public sealed class ObservabilityTests
{
    private sealed class ObservedMessage
    {
        public string Value { get; set; } = "";
    }

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

    private static bool HasTag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key, object? value)
    {
        foreach (var tag in tags)
            if (tag.Key == key && Equals(tag.Value, value))
                return true;
        return false;
    }
}
