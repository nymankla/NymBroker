namespace NymBroker.Core.Idempotency;

public interface IIdempotencyStore
{
    /// <summary>Returns true if this ID is new (proceed); false if already seen (duplicate).</summary>
    bool TryMarkSeen(Guid messageId);
}
