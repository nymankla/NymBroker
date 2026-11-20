using NymBroker.Core.Endpoint;

namespace NymBroker.Tests;

/// <summary>
/// Waits for a state with a deadline, for integration tests that share Docker services and the machine with the
/// whole suite: a fixed delay or a single 5 s health probe is too tight under that load (#128).
/// </summary>
internal static class TestWait
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Polls <paramref name="condition"/> until it is true; throws <see cref="TimeoutException"/> after the deadline.</summary>
    public static async Task UntilAsync(Func<Task<bool>> condition, TimeSpan? timeout = null, TimeSpan? interval = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Condition not met in time.");
            await Task.Delay(interval ?? TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// Repeats the endpoint's health check until it is healthy. The first check of a SQL endpoint also opens a
    /// connection and creates the table, which can take longer than the probe's own timeout; schema creation is
    /// idempotent, so repeating it is safe.
    /// </summary>
    public static async Task UntilHealthyAsync(IEndPoint endpoint, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        while (true)
        {
            var result = await endpoint.HealthCheckAsync(TestContext.Current.CancellationToken);
            if (result.IsHealthy) return;
            if (DateTime.UtcNow > deadline) Assert.Fail($"Endpoint not healthy in time: {result.Message}");
            await Task.Delay(250, TestContext.Current.CancellationToken);
        }
    }
}
