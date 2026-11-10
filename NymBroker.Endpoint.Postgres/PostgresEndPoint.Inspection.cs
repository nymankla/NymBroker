using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.Postgres;

public sealed partial class PostgresEndPoint
{
    public async Task<QueueCounts> GetCountsAsync(CancellationToken ct = default)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresQueueSql.InspectCounts(_settings.TableName);
        return await QueueInspection.ReadCountsAsync(cmd, ct);
    }

    public async Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
        DateTimeOffset? failedSince = null, CancellationToken ct = default)
    {
        QueueInspection.ValidateListArguments(state, limit);
        var (after1, after2) = QueueInspection.ParseCursor(state, cursor, ticksMustBeDateTime: true);
        var byId = QueueInspection.IsKeyedById(state);
        var afterId = byId ? after1 : after2;
        DateTimeOffset? afterTime = cursor is null || byId ? null : new DateTimeOffset(after1, TimeSpan.Zero);

        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresQueueSql.InspectList(_settings.TableName, state);
        cmd.Parameters.Add(new NpgsqlParameter<long>("afterId", NpgsqlDbType.Bigint) { TypedValue = afterId });
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset?>("afterTime", NpgsqlDbType.TimestampTz) { TypedValue = afterTime });
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset?>("since", NpgsqlDbType.TimestampTz)
        {
            TypedValue = failedSince is { } since
                ? DateTimeOffset.FromUnixTimeSeconds(since.ToUnixTimeSeconds())
                : null
        });
        cmd.Parameters.Add(new NpgsqlParameter<int>("limit", NpgsqlDbType.Integer) { TypedValue = limit + 1 });

        return await QueueInspection.ReadPageAsync(cmd, state, limit, new QueueInspection.RowReader(
            r => QueueInspection.ReadTimestampKey(r, state),
            r => QueueInspection.ReadMessage(r, state, r.GetGuid(2).ToString())), ct);
    }
}
