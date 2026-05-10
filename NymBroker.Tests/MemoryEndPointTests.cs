using System.Text;
using NymBroker.Core.Endpoint.Memory;

namespace NymBroker.Tests;

public sealed class MemoryEndPointTests
{
    [Fact]
    public async Task PostAndListen_DeliveryMessage()
    {
        var ep = new MemoryQueueEndPoint("test");
        string? received = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        await ep.StartListeningAsync(async (msg, _) =>
        {
            received = Encoding.UTF8.GetString(msg);
            cts.Cancel();
            await Task.CompletedTask;
        }, cts.Token);

        var payload = "{\"test\":true}";
        await ep.PostAsync(new MemoryStream(Encoding.UTF8.GetBytes(payload)), cts.Token);

        await Task.Delay(200);
        Assert.Equal(payload, received);
    }

    [Fact]
    public async Task ReadAsync_ReturnsQueuedItems()
    {
        var ep = new MemoryQueueEndPoint("test2");
        await ep.EnqueueAsync("{\"n\":1}");
        await ep.EnqueueAsync("{\"n\":2}");

        var items = new List<string>();
        await foreach (var item in ep.ReadAsync())
            items.Add(item);

        Assert.Equal(2, items.Count);
    }
}
