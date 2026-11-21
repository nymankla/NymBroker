using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.Postgres;

// IQueueReplayer and IQueueFailedRemover: one guarded UPDATE / DELETE … RETURNING each (one implicit transaction). Replay
// also sends a NOTIFY so idle listeners pick the rows up.
public sealed partial class PostgresEndPoint
{
    public async Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);
        var sql = PostgresQueueSql.ReplayFailed(_settings.TableName, _settings.UseNotifications);
        return QueueReplaySelection.Result(items, await ChangeFailedAsync(sql, items, ct));
    }

    public async Task<QueueRemoveResult> RemoveFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);
        var sql = PostgresQueueSql.RemoveFailed(_settings.TableName);
        return QueueReplaySelection.RemoveResult(items, await ChangeFailedAsync(sql, items, ct));
    }

    /// <summary>Runs a statement over the selection (<c>@ids</c>, <c>@failedAts</c>) and returns the message ids it changed.</summary>
    private async Task<HashSet<Guid>> ChangeFailedAsync(string sql, List<QueueReplaySelection.Item> items, CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]>("ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid)
        {
            TypedValue = [.. items.Select(i => i.Id)]
        });
        cmd.Parameters.Add(new NpgsqlParameter<DateTime[]>("failedAts", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
        {
            TypedValue = [.. items.Select(i => i.FailedAtUtc)]
        });

        var changed = new HashSet<Guid>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            changed.Add(reader.GetGuid(0));
        return changed;
    }
}
