using System.Data;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.SqlServer;

public sealed partial class SqlServerEndPoint
{
    public async Task<QueueCounts> GetCountsAsync(CancellationToken ct = default)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var command = conn.CreateCommand();
        command.CommandText = SqlServerQueueSql.InspectCounts(_settings.TableName);
        return await QueueInspection.ReadCountsAsync(command, ct);
    }

    public async Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
        DateTimeOffset? failedSince = null, CancellationToken ct = default)
    {
        QueueInspection.ValidateListArguments(state, limit);
        var (after1, after2) = QueueInspection.ParseCursor(state, cursor, ticksMustBeDateTime: true);
        var hasCursor = cursor is not null;
        var afterTime = hasCursor && (state is QueueInspectionState.Failed or QueueInspectionState.Deferred)
            ? new DateTime(after1, DateTimeKind.Utc)
            : DateTime.MinValue;
        var since = DateTimeOffset.FromUnixTimeSeconds(failedSince?.ToUnixTimeSeconds() ?? 0).UtcDateTime;

        await using var conn = await OpenConnectionAsync(ct);
        await using var command = conn.CreateCommand();
        command.CommandText = SqlServerQueueSql.InspectList(_settings.TableName, state);
        command.Parameters.Add("@after1", SqlDbType.BigInt).Value = after1;
        command.Parameters.Add("@after2", SqlDbType.BigInt).Value = after2;
        command.Parameters.Add("@hasCursor", SqlDbType.Bit).Value = hasCursor;
        command.Parameters.Add("@afterTime", SqlDbType.DateTime2).Value = afterTime;
        command.Parameters.Add("@since", SqlDbType.DateTime2).Value = since;
        command.Parameters.Add("@limit", SqlDbType.Int).Value = limit + 1;

        return await QueueInspection.ReadPageAsync(command, state, limit, new QueueInspection.RowReader(
            r => QueueInspection.ReadTimestampKey(r, state),
            r => QueueInspection.ReadMessage(r, state, r.GetGuid(2).ToString())), ct);
    }
}
