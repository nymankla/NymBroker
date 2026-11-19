using NymBroker.Core.Resilience;

namespace NymBroker.Core.Consume;

/// <summary>Options of a consumer (<c>NymBrokerBuilder.AddConsumer&lt;TConsumer&gt;(configure)</c>).</summary>
public sealed class ConsumerOptions
{
    /// <summary>
    /// Whether a message whose consumer throws is redelivered by its transport. Default true. False dead-letters it at
    /// once (reason <c>ConsumerFailed</c>) when it came from an endpoint with its own dead-letter queue (the SQL queue
    /// tables, RabbitMQ, Service Bus) instead of returning it for retry. Messages from other endpoints are posted to the
    /// broker's dead-letter endpoint either way, without a retry.
    /// </summary>
    public bool RetryOnFailure { get; set; } = true;

    /// <summary>
    /// How long one call of the consumer may take. Null (default): no limit. When it passes, the consumer's token is
    /// cancelled and the broker stops waiting for it: the message is settled as a consumer failure with reason
    /// <c>ConsumerTimedOut</c> (retried or dead-lettered as <see cref="RetryOnFailure"/> says). A consumer that ignores
    /// its token keeps running in the background. Must be above zero and at most 1 day.
    /// </summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>
    /// A circuit breaker for this consumer, shared by every message type it consumes. Null (default): none. While the
    /// circuit is open the broker holds the message (its listener waits) until the break is over and then tries it,
    /// instead of failing it: a failed message would be redelivered at once and use up its delivery count.
    /// </summary>
    public CircuitBreakerOptions? CircuitBreaker { get; set; }

    /// <summary>
    /// At most this many calls of the consumer per period, shared by every message type it consumes. Null (default):
    /// no limit. A message over the limit waits in the broker (its listener waits) instead of failing, so it never uses
    /// up its delivery count. Per process: on three instances the overall rate is three times this.
    /// </summary>
    public RateLimitOptions? RateLimit { get; set; }

    /// <summary>
    /// At most this many calls of the consumer at the same time, across every endpoint that feeds it. Null (default): no
    /// limit beyond the endpoints' own. At least 1. A message over the limit waits, like <see cref="RateLimit"/>.
    /// </summary>
    public int? ConcurrencyLimit { get; set; }

    /// <summary>Throws for an invalid <see cref="Timeout"/> or <see cref="CircuitBreaker"/>.</summary>
    internal void Validate()
    {
        if (Timeout is { } timeout)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero, nameof(Timeout));
            ArgumentOutOfRangeException.ThrowIfGreaterThan(timeout, TimeSpan.FromDays(1), nameof(Timeout));
        }
        if (CircuitBreaker is { } breaker)
            Resilience.CircuitBreaker.Validate(breaker);
        RateLimit?.Validate();
        if (ConcurrencyLimit is { } limit)
            ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1, nameof(ConcurrencyLimit));
    }

    /// <summary>The options as registered: a copy, so a later change to this instance has no effect.</summary>
    internal ConsumerSettings ToSettings() => new(RetryOnFailure, Timeout, CircuitBreaker, RateLimit, ConcurrencyLimit);
}

/// <summary>A consumer's options as registered (see <see cref="ConsumerOptions"/>).</summary>
internal sealed record ConsumerSettings(
    bool RetryOnFailure, TimeSpan? Timeout, CircuitBreakerOptions? CircuitBreaker, RateLimitOptions? RateLimit = null, int? ConcurrencyLimit = null)
{
    public static readonly ConsumerSettings Default = new(true, null, null);

    /// <summary>Whether the consumer needs a <c>ConsumerResilience</c>: any option besides <see cref="RetryOnFailure"/>.</summary>
    public bool HasResilience => Timeout is not null || CircuitBreaker is not null || RateLimit is not null || ConcurrencyLimit is not null;

    /// <summary>The same options: a breaker counts as the same when every option has the same value.</summary>
    public bool SameAs(ConsumerSettings other)
        => RetryOnFailure == other.RetryOnFailure && Timeout == other.Timeout && SameBreaker(CircuitBreaker, other.CircuitBreaker)
           && (RateLimit is null ? other.RateLimit is null : RateLimit.SameAs(other.RateLimit)) && ConcurrencyLimit == other.ConcurrencyLimit;

    private static bool SameBreaker(CircuitBreakerOptions? a, CircuitBreakerOptions? b)
        => ReferenceEquals(a, b) || (a is not null && b is not null
            && a.FailureRatio.Equals(b.FailureRatio) && a.MinimumThroughput == b.MinimumThroughput
            && a.SamplingDuration == b.SamplingDuration && a.BreakDuration == b.BreakDuration
            && a.ShouldHandle == b.ShouldHandle && a.TimeProvider == b.TimeProvider);

    public override string ToString()
        => $"RetryOnFailure = {RetryOnFailure}, Timeout = {Timeout?.ToString() ?? "none"}, CircuitBreaker = {(CircuitBreaker is null ? "none" : "set")}, " +
           $"RateLimit = {RateLimit?.ToString() ?? "none"}, ConcurrencyLimit = {ConcurrencyLimit?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "none"}";
}
