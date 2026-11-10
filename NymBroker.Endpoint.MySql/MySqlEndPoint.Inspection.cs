using MySqlConnector;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.MySql;

public sealed partial class MySqlEndPoint
{
    private static readonly DateTime MySqlMinimumDate = new(1000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public async Task<QueueCounts> GetCountsAsync(CancellationToken ct = default)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var command = new MySqlCommand(MySqlQueueSql.InspectCounts(_table), conn);
        return await QueueInspection.ReadCountsAsync(command, ct);
    }

    public async Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
        DateTimeOffset? failedSince = null, CancellationToken ct = default)
    {
        QueueInspection.ValidateListArguments(state, limit);
        var (after1, after2) = QueueInspection.ParseCursor(state, cursor);
        // A timestamp past DateTime's range: nothing can come after it.
        if (state is QueueInspectionState.Failed or QueueInspectionState.Deferred && after1 > DateTime.MaxValue.Ticks)
            return new QueuePage([], null);

        await using var conn = await OpenConnectionAsync(ct);
        await using var command = new MySqlCommand(MySqlQueueSql.InspectList(_table, state), conn);
        command.Parameters.AddWithValue("@limit", limit + 1);
        if (QueueInspection.IsKeyedById(state))
        {
            command.Parameters.AddWithValue("@after1", after1);
        }
        else
        {
            var afterTime = new DateTime(Math.Max(after1, MySqlMinimumDate.Ticks), DateTimeKind.Utc);
            command.Parameters.AddWithValue("@afterTime", afterTime);
            command.Parameters.AddWithValue("@after2", after2);
            if (state == QueueInspectionState.Failed)
            {
                var since = failedSince is { } f
                    ? DateTime.SpecifyKind(DateTimeOffset.FromUnixTimeSeconds(f.ToUnixTimeSeconds()).UtcDateTime, DateTimeKind.Utc)
                    : MySqlMinimumDate;
                command.Parameters.AddWithValue("@since", since < MySqlMinimumDate ? MySqlMinimumDate : since);
            }
        }

        // MessageId is BINARY(16).
        return await QueueInspection.ReadPageAsync(command, state, limit,
            r => QueueInspection.ReadTimestampKey(r, state),
            r => QueueInspection.ReadMessage(r, state, new Guid(r.GetFieldValue<byte[]>(2)).ToString()), ct);
    }
}
