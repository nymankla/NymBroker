namespace MessageBroker.Core.Aggregator;

public sealed class SplitMessage
{
    public Guid CorrelationId { get; set; }
    public int CorrelationSequence { get; set; }
    public int GroupSize { get; set; }

    /// <summary>Base64-encoded chunk of the original payload.</summary>
    public string Body { get; set; } = string.Empty;
}
