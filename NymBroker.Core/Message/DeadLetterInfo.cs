namespace NymBroker.Core.Message;

/// <summary>Why a message was dead-lettered; carried in the optional <c>deadLetter</c> block of the envelope.</summary>
/// <param name="Reason">One of <see cref="Endpoint.DeadLetterReasons"/>, or a transport reason such as <c>MaxDeliveryCountExceeded</c>.</param>
/// <param name="Description">The failure message (no stack trace), capped at 4 096 characters.</param>
/// <param name="ExceptionType">Full type name of the exception that caused it, if any.</param>
/// <param name="SourceEndpoint">The endpoint the message was received from.</param>
/// <param name="DeadLetteredAt">When the message was dead-lettered (UTC).</param>
/// <param name="DeliveryCount">Delivery attempts, when the transport knows it; otherwise null.</param>
public sealed record DeadLetterInfo(
    string Reason,
    string? Description,
    string? ExceptionType,
    string? SourceEndpoint,
    DateTime DeadLetteredAt,
    int? DeliveryCount = null);
