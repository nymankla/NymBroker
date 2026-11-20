using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.Postgres;

// IQueueReplayer: one UPDATE … RETURNING (one implicit transaction), plus a NOTIFY so idle listeners pick the rows up.
public sealed partial class PostgresEndPoint
{
    public async Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);

        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresQueueSql.ReplayFailed(_settings.TableName, _settings.UseNotifications);
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]>("ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid)
        {
            TypedValue = [.. items.Select(i => i.Id)]
        });
        cmd.Parameters.Add(new NpgsqlParameter<DateTime[]>("failedAts", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
        {
            TypedValue = [.. items.Select(i => i.FailedAtUtc)]
        });

        var replayed = new HashSet<Guid>();
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                replayed.Add(reader.GetGuid(0));
        }
        return QueueReplaySelection.Result(items, replayed);
    }
}
