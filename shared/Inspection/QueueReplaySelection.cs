using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Shared;

/// <summary>
/// Provider-neutral parts of the SQL endpoints' <see cref="IQueueReplayer"/> and <see cref="IQueueFailedRemover"/>: the argument checks and turning the selection
/// into (id, failed at) keys, and the rows the statement changed into a result. The UPDATE / DELETE
/// stays in each endpoint.
/// </summary>
/// <remarks>
/// A row is replayed or removed only when it is still Failed with the listed failure time. That makes the failure time an
/// optimistic-concurrency token (a row replayed already, or failed again since, no longer matches) and lets the statement
/// find the row through the failed-rows index instead of the unindexed message id.
/// </remarks>
internal static class QueueReplaySelection
{
    /// <summary>One selected row: its message id, the id as the caller gave it, and the failure time in UTC.</summary>
    internal readonly record struct Item(Guid Id, string Text, DateTime FailedAtUtc);

    /// <summary>Checks the selection and returns one item per distinct message id, in the given order.</summary>
    internal static List<Item> Parse(IReadOnlyCollection<QueueMessageInfo> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentOutOfRangeException.ThrowIfZero(messages.Count, nameof(messages));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(messages.Count, IQueueInspector.MaxPageSize, nameof(messages));

        var items = new List<Item>(messages.Count);
        var seen = new HashSet<Guid>();
        foreach (var message in messages)
        {
            if (message is not { State: QueueInspectionState.Failed, FailedAt: { } failedAt })
                throw new ArgumentException($"Message '{message?.Id}' is not a failed message with a failure time; only failed messages can be replayed or removed.", nameof(messages));
            if (!Guid.TryParse(message.Id, out var id))
                throw new ArgumentException($"'{message.Id}' is not a message id of this endpoint.", nameof(messages));
            if (seen.Add(id))
                items.Add(new Item(id, message.Id, failedAt.UtcDateTime));
        }
        return items;
    }

    /// <summary>The selection split into the ids the statement replayed and the rest.</summary>
    internal static QueueReplayResult Result(List<Item> items, IReadOnlySet<Guid> replayed)
    {
        var (done, skipped) = Split(items, replayed);
        return new QueueReplayResult(done, skipped);
    }

    /// <summary>The selection split into the ids the statement removed and the rest.</summary>
    internal static QueueRemoveResult RemoveResult(List<Item> items, IReadOnlySet<Guid> removed)
    {
        var (done, skipped) = Split(items, removed);
        return new QueueRemoveResult(done, skipped);
    }

    /// <summary>Splits the selection into the ids the statement changed and the rest, both in the selection's order and spelling.</summary>
    private static (List<string> Done, List<string> Skipped) Split(List<Item> items, IReadOnlySet<Guid> changed)
    {
        var done = new List<string>(changed.Count);
        var skipped = new List<string>(items.Count - changed.Count);
        foreach (var item in items)
            (changed.Contains(item.Id) ? done : skipped).Add(item.Text);
        return (done, skipped);
    }

    /// <summary>The selection as JSON for providers that join it through a JSON table function: <c>[{"id": "…", "at": "…"}]</c>.</summary>
    internal static string ToJson(List<Item> items, Func<Item, string> id, Func<Item, string> failedAt)
    {
        var rows = items.Select(i => new Dictionary<string, string> { ["id"] = id(i), ["at"] = failedAt(i) });
        return System.Text.Json.JsonSerializer.Serialize(rows);
    }
}
