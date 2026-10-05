namespace NymBroker.Core.Endpoint;

/// <summary>What the source endpoint should do with a received message after <c>ProcessAsync</c>.</summary>
public enum ProcessOutcome
{
    /// <summary>Done — ack / complete / delete the message.</summary>
    Completed,

    /// <summary>Processing failed transiently — let the transport redeliver it (nack, abandon, back to Pending).</summary>
    Retry,

    /// <summary>The message can never succeed — dead-letter it natively (only returned to endpoints with <see cref="IEndPointEventDriven.UsesNativeDeadLetter"/>).</summary>
    DeadLetter
}

/// <summary>Result of processing one received message; returned to the source endpoint's listener.</summary>
/// <param name="Outcome">How the endpoint should settle the message.</param>
/// <param name="Reason">For <see cref="ProcessOutcome.DeadLetter"/>: one of <see cref="DeadLetterReasons"/>.</param>
/// <param name="Description">Human-readable detail, typically the exception message.</param>
/// <param name="Exception">The exception behind a <see cref="ProcessOutcome.Retry"/> or <see cref="ProcessOutcome.DeadLetter"/>, if any.</param>
public readonly record struct ProcessResult(
    ProcessOutcome Outcome,
    string? Reason = null,
    string? Description = null,
    Exception? Exception = null)
{
    public static ProcessResult Completed { get; } = new(ProcessOutcome.Completed);

    public static ProcessResult Retry(Exception? exception = null, string? description = null)
        => new(ProcessOutcome.Retry, null, description ?? exception?.Message, exception);

    public static ProcessResult DeadLetter(string reason, string? description = null, Exception? exception = null)
        => new(ProcessOutcome.DeadLetter, reason, description ?? exception?.Message, exception);

    /// <summary><c>"{Reason}: {Description}"</c> (or whichever part is present) — for logs and error columns.</summary>
    public string? FailureText
        => (Reason, Description) switch
        {
            (null, null) => null,
            (null, var d) => d,
            (var r, null) => r,
            var (r, d) => $"{r}: {d}"
        };
}

/// <summary>Reasons the broker gives when it asks an endpoint to dead-letter a message.</summary>
public static class DeadLetterReasons
{
    /// <summary>The bytes could not be deserialized into a message envelope.</summary>
    public const string DeserializationFailed = "DeserializationFailed";

    /// <summary>The message was older than the configured maximum age (<c>DiscardMessagesOlderThan</c>).</summary>
    public const string Expired = "Expired";

    /// <summary>A reassembled split message used a compression codec this broker does not have.</summary>
    public const string UnknownCompression = "UnknownCompression";

    /// <summary>A consumer threw. Only used on the broker dead-letter endpoint path; native endpoints get <see cref="ProcessOutcome.Retry"/>.</summary>
    public const string ConsumerFailed = "ConsumerFailed";

    /// <summary>Topic fan-out failed. Only used on the broker dead-letter endpoint path; native endpoints get <see cref="ProcessOutcome.Retry"/>.</summary>
    public const string TopicDeliveryFailed = "TopicDeliveryFailed";
}
