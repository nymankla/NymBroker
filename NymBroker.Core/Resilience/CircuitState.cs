namespace NymBroker.Core.Resilience;

/// <summary>The state of a <see cref="CircuitBreaker"/>.</summary>
public enum CircuitState
{
    /// <summary>Calls go through; their outcomes are counted.</summary>
    Closed,

    /// <summary>Too many calls failed: calls are refused until the break duration has passed.</summary>
    Open,

    /// <summary>The break is over: one trial call decides whether the circuit closes or opens again.</summary>
    HalfOpen
}
