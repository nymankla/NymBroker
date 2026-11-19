using System.Diagnostics;

namespace NymBroker.Core.Resilience;

/// <summary>
/// Thread-safe circuit breaker. Closed: calls go through and their outcomes are counted over a rolling window; when the
/// share of failures reaches <see cref="CircuitBreakerOptions.FailureRatio"/> (with at least
/// <see cref="CircuitBreakerOptions.MinimumThroughput"/> calls) it opens. Open: calls are refused for
/// <see cref="CircuitBreakerOptions.BreakDuration"/>. Half-open: one trial call; success closes it, failure opens it again.
/// Build once per protected resource and reuse.
/// </summary>
public sealed class CircuitBreaker
{
    private const int BucketCount = 10;

    private readonly CircuitBreakerOptions _options;
    private readonly TagList _tags;
    private readonly long _bucketTicks;
    private readonly Lock _lock = new();

    // Guarded by _lock.
    private readonly long[] _bucketIds = new long[BucketCount];
    private readonly int[] _successes = new int[BucketCount];
    private readonly int[] _failures = new int[BucketCount];
    private CircuitState _state = CircuitState.Closed;
    private DateTimeOffset _openUntil;
    private TaskCompletionSource? _trial;   // set while a half-open trial call runs

    public CircuitBreaker(CircuitBreakerOptions options) : this(options, "default") { }

    internal CircuitBreaker(CircuitBreakerOptions options, string name)
    {
        Validate(options);
        _options = options;
        Name = name;
        _tags = new TagList { { "circuit", name } };
        _bucketTicks = options.SamplingDuration.Ticks / BucketCount;
        Array.Fill(_bucketIds, long.MinValue);
    }

    /// <summary>The circuit's name in metrics (<c>circuit</c> tag) and logs.</summary>
    internal string Name { get; }

    /// <summary>Called after each state change, outside the breaker's lock. Must not throw.</summary>
    internal Action<CircuitState>? StateChanged { get; init; }

    internal TimeProvider TimeProvider => _options.TimeProvider;

    /// <summary>The current state. An open circuit whose break is over reports <see cref="CircuitState.HalfOpen"/>.</summary>
    public CircuitState State
    {
        get
        {
            lock (_lock)
                return _state == CircuitState.Open && Now >= _openUntil ? CircuitState.HalfOpen : _state;
        }
    }

    /// <summary>Throws for invalid options; the same checks as the constructor.</summary>
    internal static void Validate(CircuitBreakerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!(options.FailureRatio > 0 && options.FailureRatio <= 1))
            throw new ArgumentOutOfRangeException(nameof(options), options.FailureRatio, "FailureRatio must be above 0 and at most 1.");
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MinimumThroughput, 1, nameof(options.MinimumThroughput));
        ArgumentOutOfRangeException.ThrowIfLessThan(options.SamplingDuration, TimeSpan.FromMilliseconds(10), nameof(options.SamplingDuration));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.BreakDuration, TimeSpan.Zero, nameof(options.BreakDuration));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(options.BreakDuration, TimeSpan.FromDays(1), nameof(options.BreakDuration));
        ArgumentNullException.ThrowIfNull(options.ShouldHandle, nameof(options.ShouldHandle));
        ArgumentNullException.ThrowIfNull(options.TimeProvider, nameof(options.TimeProvider));
    }

    // RS0026: the overloads differ in a required parameter (the action's result type), as RetryPolicy.ExecuteAsync's do.
#pragma warning disable RS0026
    /// <summary>Runs the action if the circuit allows it; otherwise throws <see cref="BrokenCircuitException"/> at once.</summary>
    public async ValueTask ExecuteAsync(Func<CancellationToken, ValueTask> action, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(action);
        var permit = AcquireOrThrow();
        try
        {
            await action(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            RecordIgnored(permit);
            throw;
        }
        catch (Exception ex)
        {
            RecordException(permit, ex);
            throw;
        }
        RecordSuccess(permit);
    }

    /// <summary>Runs the action if the circuit allows it; otherwise throws <see cref="BrokenCircuitException"/> at once.</summary>
    public async ValueTask<T> ExecuteAsync<T>(Func<CancellationToken, ValueTask<T>> action, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(action);
        var permit = AcquireOrThrow();
        T result;
        try
        {
            result = await action(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            RecordIgnored(permit);
            throw;
        }
        catch (Exception ex)
        {
            RecordException(permit, ex);
            throw;
        }
        RecordSuccess(permit);
        return result;
    }

#pragma warning restore RS0026

    private Permit AcquireOrThrow()
    {
        if (TryAcquire(out var permit, out var wait)) return permit;
        ResilienceDiagnostics.CircuitRejected.Add(1, _tags);
        throw wait.Trial is null
            ? new BrokenCircuitException($"The circuit '{Name}' is open.", wait.Delay)
            : new BrokenCircuitException($"The circuit '{Name}' is half-open and its trial call is still running.", retryAfter: null);
    }

    /// <summary>
    /// Waits until the circuit lets a call through (backpressure instead of rejection). Returns the permit and whether it
    /// had to wait. Throws <see cref="OperationCanceledException"/> when <paramref name="ct"/> is cancelled while waiting.
    /// </summary>
    internal async ValueTask<(Permit Permit, bool Waited)> WaitForPermitAsync(CancellationToken ct)
    {
        var waited = false;
        while (true)
        {
            if (TryAcquire(out var permit, out var wait)) return (permit, waited);
            if (!waited) ResilienceDiagnostics.CircuitHeld.Add(1, _tags);
            waited = true;
            if (wait.Trial is { } trial)
                await trial.WaitAsync(ct).ConfigureAwait(false);
            else
                await Task.Delay(wait.Delay, _options.TimeProvider, ct).ConfigureAwait(false);
        }
    }

    /// <summary>A call let through: <see cref="IsTrial"/> when it is the half-open trial.</summary>
    internal readonly record struct Permit(bool IsTrial);

    /// <summary>Why a call has to wait: until <see cref="Delay"/> has passed, or until the running trial ends.</summary>
    private readonly record struct Wait(TimeSpan Delay, Task? Trial);

    private bool TryAcquire(out Permit permit, out Wait wait)
    {
        CircuitState? changed = null;
        bool acquired;
        lock (_lock)
        {
            var now = Now;
            if (_state == CircuitState.Open && now >= _openUntil)
                changed = _state = CircuitState.HalfOpen;

            (acquired, permit, wait) = _state switch
            {
                CircuitState.Closed => (true, new Permit(false), default),
                CircuitState.Open => (false, default, new Wait(_openUntil - now, null)),
                _ when _trial is null => StartTrial(),
                _ => (false, default, new Wait(TimeSpan.Zero, _trial.Task))
            };
        }
        Notify(changed);
        return acquired;
    }

    private (bool, Permit, Wait) StartTrial()
    {
        _trial = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        return (true, new Permit(true), default);
    }

    internal void RecordSuccess(Permit permit) => Record(permit, Outcome.Success);

    /// <summary>A failure if <see cref="CircuitBreakerOptions.ShouldHandle"/> says so; any other exception is not counted.</summary>
    internal void RecordException(Permit permit, Exception ex) => Record(permit, _options.ShouldHandle(ex) ? Outcome.Failure : Outcome.Ignored);

    /// <summary>The call did not run to an outcome (e.g. the caller was cancelled): frees a trial slot, counts nothing.</summary>
    internal void RecordIgnored(Permit permit) => Record(permit, Outcome.Ignored);

    private enum Outcome { Success, Failure, Ignored }

    private void Record(Permit permit, Outcome outcome)
    {
        CircuitState? changed;
        TaskCompletionSource? trialEnded = null;
        lock (_lock)
        {
            if (permit.IsTrial)
            {
                trialEnded = _trial;
                _trial = null;
                changed = EndTrial(outcome);
            }
            else
            {
                changed = CountInWindow(outcome);
            }
        }
        trialEnded?.TrySetResult();
        Notify(changed);
    }

    private CircuitState? EndTrial(Outcome outcome)
    {
        if (_state != CircuitState.HalfOpen) return null;
        switch (outcome)
        {
            case Outcome.Success:
                ResetWindow();
                return _state = CircuitState.Closed;
            case Outcome.Failure:
                return Open();
            default:
                return null;   // stays half-open; the next caller is the trial
        }
    }

    // A call let through while closed; its outcome no longer matters if the circuit opened meanwhile.
    private CircuitState? CountInWindow(Outcome outcome)
    {
        if (_state != CircuitState.Closed || outcome == Outcome.Ignored) return null;

        var bucket = CurrentBucket();
        if (outcome == Outcome.Success)
        {
            _successes[bucket]++;
            return null;
        }

        _failures[bucket]++;
        var (successes, failures) = WindowTotals();
        var total = successes + failures;
        return total >= _options.MinimumThroughput && failures >= _options.FailureRatio * total ? Open() : null;
    }

    private CircuitState Open()
    {
        _openUntil = Now + _options.BreakDuration;
        return _state = CircuitState.Open;
    }

    private int CurrentBucket()
    {
        var id = Now.UtcTicks / _bucketTicks;
        var slot = (int)(id % BucketCount);
        if (_bucketIds[slot] != id)
        {
            _bucketIds[slot] = id;
            _successes[slot] = 0;
            _failures[slot] = 0;
        }
        return slot;
    }

    private (int Successes, int Failures) WindowTotals()
    {
        var oldest = Now.UtcTicks / _bucketTicks - BucketCount + 1;
        int successes = 0, failures = 0;
        for (var i = 0; i < BucketCount; i++)
        {
            if (_bucketIds[i] < oldest) continue;
            successes += _successes[i];
            failures += _failures[i];
        }
        return (successes, failures);
    }

    private void ResetWindow()
    {
        Array.Fill(_bucketIds, long.MinValue);
        Array.Clear(_successes);
        Array.Clear(_failures);
    }

    private void Notify(CircuitState? changed)
    {
        if (changed is not { } state) return;
        var tags = _tags;
        tags.Add("state", StateTag(state));
        ResilienceDiagnostics.CircuitStateChanges.Add(1, tags);
        StateChanged?.Invoke(state);
    }

    internal static string StateTag(CircuitState state) => state switch
    {
        CircuitState.Closed => "closed",
        CircuitState.Open => "open",
        _ => "half_open"
    };

    private DateTimeOffset Now => _options.TimeProvider.GetUtcNow();
}
