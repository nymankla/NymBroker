using System.Text;
using MessageBroker.Core.Endpoint.Memory;

namespace MessageBroker.Tests;

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
        await ep.StartListeningAsync((_, _) => Task.CompletedTask, CancellationToken.None);

        await ep.StopListeningAsync();

        // After stopping, the channel writer is completed; no items should be readable.
        var items = new List<string>();
        await foreach (var item in ep.ReadAsync()) items.Add(item);
        Assert.Empty(items);
    }

    [Fact]
    public async Task PostAsync_And_ReadAsync_MultipleMessages_PreservesOrder()
    {
        var ep = new MemoryQueueEndPoint("order-test");
        var payloads = new[] { "{\"n\":1}", "{\"n\":2}", "{\"n\":3}" };

        foreach (var p in payloads)
            await ep.PostAsync(new MemoryStream(Encoding.UTF8.GetBytes(p)));

        var received = new List<string>();
        await foreach (var item in ep.ReadAsync()) received.Add(item);

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
                received.Add(msg);
                if (received.Count == 3)
                    allReceived.TrySetResult();
            }
            await Task.CompletedTask;
        }, cts.Token);

        await ep.EnqueueAsync("{\"n\":1}");
        await ep.EnqueueAsync("{\"n\":2}");
        await ep.EnqueueAsync("{\"n\":3}");

        await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(3)));

        Assert.Equal(3, received.Count);
    }

    [Fact]
    public void Name_ReturnsConstructorValue()
    {
        var ep = new MemoryQueueEndPoint("my-queue");
        Assert.Equal("my-queue", ep.Name);
    }

    [Fact]
    public async Task ReadAsync_RespectsGivenCancellationToken()
    {
        var ep = new MemoryQueueEndPoint("cancel-test");
        await ep.EnqueueAsync("{\"a\":1}");

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var items = new List<string>();
        await foreach (var item in ep.ReadAsync(cts.Token)) items.Add(item);

        // Cancelled token stops iteration before yielding any item.
        Assert.Empty(items);
    }
}
