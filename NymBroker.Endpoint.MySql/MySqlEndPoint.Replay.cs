using System.Globalization;
using MySqlConnector;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.MySql;

// IQueueReplayer and IQueueFailedRemover: MySQL's UPDATE and DELETE have no RETURNING, so one transaction locks the
// matching rows (and reads their ids), then updates or deletes them by primary key and commits.
public sealed partial class MySqlEndPoint
{
    public async Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);
        var replayed = await ChangeFailedAsync(items, ids => MySqlQueueSql.ReplayLocked(_table, ids), ct);
        return QueueReplaySelection.Result(items, replayed);
    }

    public async Task<QueueRemoveResult> RemoveFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);
        var removed = await ChangeFailedAsync(items, ids => MySqlQueueSql.RemoveLocked(_table, ids), ct);
        return QueueReplaySelection.RemoveResult(items, removed);
    }

    /// <summary>Locks the selected rows still failed as listed, runs <paramref name="change"/> on them and commits; returns their message ids.</summary>
    private async Task<HashSet<Guid>> ChangeFailedAsync(List<QueueReplaySelection.Item> items, Func<IReadOnlyList<long>, string> change,
        CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        try
        {
            var (queueIds, messageIds) = await LockFailedAsync(conn, items, ct);
            await using var finish = new MySqlCommand(queueIds.Count == 0 ? "COMMIT;" : change(queueIds), conn);
            await finish.ExecuteNonQueryAsync(ct);
            return messageIds;
        }
        catch
        {
            await RollbackQuietlyAsync(conn);
            throw;
        }
    }

    private async Task<(List<long> QueueIds, HashSet<Guid> MessageIds)> LockFailedAsync(MySqlConnection conn,
        List<QueueReplaySelection.Item> items, CancellationToken ct)
    {
        var queueIds = new List<long>(items.Count);
        var messageIds = new HashSet<Guid>();
        await using var lockCmd = new MySqlCommand(MySqlQueueSql.LockFailed(_table), conn);
        // message_id is Guid.ToByteArray() in BINARY(16); DATETIME(6) has microseconds, as listed by the inspector.
        lockCmd.Parameters.AddWithValue("@items", QueueReplaySelection.ToJson(items,
            i => Convert.ToHexString(i.Id.ToByteArray()),
            i => i.FailedAtUtc.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)));
        await using var reader = await lockCmd.ExecuteReaderAsync(ct);
        do
        {
            while (await reader.ReadAsync(ct))
            {
                queueIds.Add(reader.GetInt64(0));
                messageIds.Add(new Guid(reader.GetFieldValue<byte[]>(1)));
            }
        } while (await reader.NextResultAsync(ct));
        return (queueIds, messageIds);
    }
}
