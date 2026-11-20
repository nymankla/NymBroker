using System.Globalization;
using MySqlConnector;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.MySql;

// IQueueReplayer: MySQL's UPDATE has no RETURNING, so one transaction locks the matching rows (and reads their ids),
// then updates them by primary key and commits.
public sealed partial class MySqlEndPoint
{
    public async Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);

        await using var conn = await OpenConnectionAsync(ct);
        try
        {
            var (queueIds, replayed) = await LockFailedForReplayAsync(conn, items, ct);
            await using var finish = new MySqlCommand(queueIds.Count == 0 ? "COMMIT;" : MySqlQueueSql.ReplayLocked(_table, queueIds), conn);
            await finish.ExecuteNonQueryAsync(ct);
            return QueueReplaySelection.Result(items, replayed);
        }
        catch
        {
            await RollbackQuietlyAsync(conn);
            throw;
        }
    }

    private async Task<(List<long> QueueIds, HashSet<Guid> MessageIds)> LockFailedForReplayAsync(MySqlConnection conn,
        List<QueueReplaySelection.Item> items, CancellationToken ct)
    {
        var queueIds = new List<long>(items.Count);
        var messageIds = new HashSet<Guid>();
        await using var lockCmd = new MySqlCommand(MySqlQueueSql.LockFailedForReplay(_table), conn);
        // message_id is Guid.ToByteArray() in BINARY(16); DATETIME(6) has microseconds, as listed by the inspector.
        lockCmd.Parameters.AddWithValue("@items", QueueReplaySelection.ToJson(items, i => Convert.ToHexString(i.Id.ToByteArray()), i => i.FailedAtUtc.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)));
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
