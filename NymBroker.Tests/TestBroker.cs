using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Impl;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using NymBroker.Core.Splitter;

namespace NymBroker.Tests;

/// <summary>A broker built by hand, without the DI builder; consumers and subscribers are resolved from <c>services</c>.</summary>
internal static class TestBroker
{
    public static NymBrokerImpl Create(IServiceProvider services, ILogger<NymBrokerImpl>? logger = null,
        MessageSerializerJson? serializer = null, ICompressor? compressor = null)
    {
        var scopes = services.GetRequiredService<IServiceScopeFactory>();
        return new NymBrokerImpl(
            serializer ?? new MessageSerializerJson(),
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(scopes, NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(scopes, NullLogger<SubscriberDispatcher>.Instance),
            logger ?? NullLogger<NymBrokerImpl>.Instance,
            compressor: compressor);
    }
}
