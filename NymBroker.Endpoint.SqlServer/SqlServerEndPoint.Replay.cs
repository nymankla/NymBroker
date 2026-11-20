using System.Data;
using System.Globalization;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.SqlServer;

// IQueueReplayer: one UPDATE … OUTPUT joined to the selection as JSON, atomic as one statement.
public sealed partial class SqlServerEndPoint
{
    public async Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);

        await using var conn = await OpenConnectionAsync(ct);
        await using var command = conn.CreateCommand();
        command.CommandText = SqlServerQueueSql.ReplayFailed(_settings.TableName);
        command.Parameters.Add("@items", SqlDbType.NVarChar, -1).Value = QueueReplaySelection.ToJson(items, i => i.Id.ToString(),
            i => i.FailedAtUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture));

        var replayed = new HashSet<Guid>();
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                replayed.Add(reader.GetGuid(0));
        }
        return QueueReplaySelection.Result(items, replayed);
    }
}
