namespace MessageBroker.Core.Aggregator;

public interface ICompletionCondition
{
    bool IsComplete(SplitMessage incoming, Aggregate aggregate);
}
