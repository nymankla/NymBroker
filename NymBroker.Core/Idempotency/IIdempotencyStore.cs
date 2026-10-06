namespace NymBroker.Core.Idempotency;

/// <summary>Outcome of <see cref="IIdempotencyStore.TryClaimAsync"/>.</summary>
public enum IdempotencyClaimResult
{
    /// <summary>New (or its earlier record expired): process it, then complete or release the claim.</summary>
    Claimed,

    /// <summary>Already processed within the TTL: drop it.</summary>
    Duplicate,

    /// <summary>Another delivery holds a live claim: try again later.</summary>
    InProgress
}

/// <summary>
/// Remembers which message IDs were processed, so an idempotent receiver can drop duplicates. Two-phase: the broker
/// claims an ID before processing, then completes the claim when the message was handled (it is remembered for the
/// TTL) or releases it when processing will be retried (so the redelivery is processed). A claim is a lease: if the
/// process dies before completing or releasing it, the claim expires and the message can be claimed again.
/// </summary>
/// <remarks>Implementations must be thread-safe and should be safe to share between broker instances.</remarks>
public interface IIdempotencyStore
{
    ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct = default);

    /// <summary>The message was handled: remember it for the TTL.</summary>
    ValueTask CompleteAsync(Guid messageId, CancellationToken ct = default);

    /// <summary>Processing failed and will be retried: forget the claim so a redelivery is processed.</summary>
    ValueTask ReleaseAsync(Guid messageId, CancellationToken ct = default);
}
