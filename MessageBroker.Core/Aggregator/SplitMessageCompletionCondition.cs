namespace MessageBroker.Core.Aggregator;

public sealed class SplitMessageCompletionCondition : ICompletionCondition
{
    public bool IsComplete(SplitMessage incoming, Aggregate aggregate)
        => aggregate.ReceivedCount >= incoming.GroupSize;
}
