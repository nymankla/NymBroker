using System.Text;
using NymBroker.Core.Endpoint.Memory;

namespace NymBroker.Tests;

public sealed class MemoryEndPointAdditionalTests
{
    [Fact]
    public void HealthCheck_ReturnsHealthy()
    {
        var ep = new MemoryQueueEndPoint("health-test");
        var result = ep.HealthCheck();
        Assert.True(result.IsHealthy);
    }

    [Fact]
    public async Task StopListeningAsync_CompletesTheChannel()
    {
        var ep = new MemoryQueueEndPoint("stop-test");
        await ep.StartListeningAsync((_, _) => Task.CompletedTask, TestContext.Current.CancellationToken);

        await ep.StopListeningAsync();

        // After stopping, the channel writer is completed; no items should be readable.
        var items = new List<string>();
        await foreach (var item in ep.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);
        Assert.Empty(items);
    }

    [Fact]
    public async Task PostAsync_And_ReadAsync_MultipleMessages_PreservesOrder()
    {
        var ep = new MemoryQueueEndPoint("order-test");
        var payloads = new[] { "{\"n\":1}", "{\"n\":2}", "{\"n\":3}" };

        foreach (var p in payloads)
            await ep.PostAsync(Encoding.UTF8.GetBytes(p), TestContext.Current.CancellationToken);

        var received = new List<string>();
        await foreach (var item in ep.ReadAsync(TestContext.Current.CancellationToken)) received.Add(item);

        Assert.Equal(payloads, received);
    }

    [Fact]
    public async Task EnqueueAsync_And_ListenAsync_DeliverAllMessages()
    {
        var ep = new MemoryQueueEndPoint("batch-test");
        var received = new List<string>();
        var allReceived = new TaskCompletionSource();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await ep.StartListeningAsync(async (msg, _) =>
        {
            lock (received)
            {
                received.Add(Encoding.UTF8.GetString(msg));
                if (received.Count == 3)
                    allReceived.TrySetResult();
            }
            await Task.CompletedTask;
        }, cts.Token);

        await ep.EnqueueAsync("{\"n\":1}", TestContext.Current.CancellationToken);
        await ep.EnqueueAsync("{\"n\":2}", TestContext.Current.CancellationToken);
        await ep.EnqueueAsync("{\"n\":3}", TestContext.Current.CancellationToken);

        await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));

        Assert.Equal(3, received.Count);
    }

    [Fact]
    public async Task ReadAsync_RespectsGivenCancellationToken()
    {
        var ep = new MemoryQueueEndPoint("cancel-test");
        await ep.EnqueueAsync("{\"a\":1}", TestContext.Current.CancellationToken);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var items = new List<string>();
        await foreach (var item in ep.ReadAsync(cts.Token)) items.Add(item);

        // Cancelled token stops iteration before yielding any item.
        Assert.Empty(items);
    }
}
