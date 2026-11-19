using System.Globalization;

namespace NymBroker.Core.Resilience;

/// <summary>
/// A rate limit: at most <see cref="PermitsPerPeriod"/> calls per <see cref="Period"/>, spread evenly, with up to
/// <see cref="Burst"/> calls at once after a quiet time. Callers over the limit wait, in the order they arrived.
/// </summary>
public sealed class RateLimitOptions
{
    /// <summary>Calls allowed per <see cref="Period"/>. At least 1.</summary>
    public int PermitsPerPeriod { get; init; } = 1;

    /// <summary>The period <see cref="PermitsPerPeriod"/> applies to. Above zero, at most 1 day. Default 1 s.</summary>
    public TimeSpan Period { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Calls allowed at once after a quiet time. Null (default): <see cref="PermitsPerPeriod"/>. At least 1.</summary>
    public int? Burst { get; init; }

    /// <summary>Clock for the waits — override in tests.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Throws for invalid options.</summary>
    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(PermitsPerPeriod, 1, nameof(PermitsPerPeriod));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(Period, TimeSpan.Zero, nameof(Period));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(Period, TimeSpan.FromDays(1), nameof(Period));
        if (Period.Ticks < PermitsPerPeriod)
            throw new ArgumentOutOfRangeException(nameof(PermitsPerPeriod), PermitsPerPeriod, "PermitsPerPeriod is more than one per tick of Period.");
        if (Burst is { } burst) ArgumentOutOfRangeException.ThrowIfLessThan(burst, 1, nameof(Burst));
        ArgumentNullException.ThrowIfNull(TimeProvider, nameof(TimeProvider));
    }

    internal int EffectiveBurst => Burst ?? PermitsPerPeriod;

    /// <summary>The same limit: every option has the same value.</summary>
    internal bool SameAs(RateLimitOptions? other)
        => other is not null && PermitsPerPeriod == other.PermitsPerPeriod && Period == other.Period
           && EffectiveBurst == other.EffectiveBurst && TimeProvider == other.TimeProvider;

    /// <summary>The limit as text, e.g. <c>50/1s, burst 50</c>.</summary>
    public override string ToString() => $"{PermitsPerPeriod}/{PeriodText(Period)}, burst {EffectiveBurst}";

    private static string PeriodText(TimeSpan period)
        => period.Ticks % TimeSpan.TicksPerSecond == 0
            ? period.TotalSeconds.ToString(CultureInfo.InvariantCulture) + "s"
            : period.TotalMilliseconds.ToString(CultureInfo.InvariantCulture) + "ms";
}

/// <summary>Limits on what the broker sends to one endpoint (<c>NymBrokerBuilder.LimitSendsTo</c>).</summary>
public sealed class SendLimitOptions
{
    /// <summary>Sends allowed per period; a batch takes one permit per message. Null: no rate limit.</summary>
    public RateLimitOptions? RateLimit { get; set; }

    /// <summary>Sends (a batch counts as one) that may run at the same time. Null: no limit. At least 1.</summary>
    public int? ConcurrencyLimit { get; set; }

    /// <summary>Throws for invalid options, or when neither limit is set.</summary>
    internal void Validate()
    {
        if (RateLimit is null && ConcurrencyLimit is null)
            throw new ArgumentException("Set RateLimit, ConcurrencyLimit or both.");
        RateLimit?.Validate();
        if (ConcurrencyLimit is { } limit) ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1, nameof(ConcurrencyLimit));
    }
}
