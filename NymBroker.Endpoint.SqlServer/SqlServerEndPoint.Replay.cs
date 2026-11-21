using System.Data;
using System.Globalization;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.SqlServer;

// IQueueReplayer and IQueueFailedRemover: one guarded UPDATE / DELETE … OUTPUT each, joined to the selection as JSON, atomic
// as one statement.
public sealed partial class SqlServerEndPoint
{
    public async Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);
        var replayed = await ChangeFailedAsync(SqlServerQueueSql.ReplayFailed(_settings.TableName), items, ct);
        return QueueReplaySelection.Result(items, replayed);
    }

    public async Task<QueueRemoveResult> RemoveFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);
        var removed = await ChangeFailedAsync(SqlServerQueueSql.RemoveFailed(_settings.TableName), items, ct);
        return QueueReplaySelection.RemoveResult(items, removed);
    }

    /// <summary>Runs a statement over the selection (<c>@items</c>) and returns the message ids it changed.</summary>
    private async Task<HashSet<Guid>> ChangeFailedAsync(string sql, List<QueueReplaySelection.Item> items, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var command = conn.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add("@items", SqlDbType.NVarChar, -1).Value = QueueReplaySelection.ToJson(items, i => i.Id.ToString(),
            i => i.FailedAtUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture));

        var changed = new HashSet<Guid>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            changed.Add(reader.GetGuid(0));
        return changed;
    }
}
