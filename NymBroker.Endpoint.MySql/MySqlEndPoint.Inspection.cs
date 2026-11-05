using System.Globalization;
using MySqlConnector;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Endpoint.MySql;

public sealed partial class MySqlEndPoint
{
    private static readonly DateTime MySqlMinimumDate = new(1000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public async Task<QueueCounts> GetCountsAsync(CancellationToken ct = default)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var command = new MySqlCommand(MySqlQueueSql.InspectCounts(_table), conn);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new QueueCounts(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(4), reader.GetInt64(2), reader.GetInt64(3));
    }

    public async Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
        DateTimeOffset? failedSince = null, CancellationToken ct = default)
    {
        if (state is < QueueInspectionState.Pending or > QueueInspectionState.Failed)
            throw new ArgumentOutOfRangeException(nameof(state), state, "Only pending, in progress, deferred and failed messages can be listed.");
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, IQueueInspector.MaxPageSize);

        var (after1, after2) = ParseCursor(state, cursor);
        if (state is QueueInspectionState.Failed or QueueInspectionState.Deferred && after1 > DateTime.MaxValue.Ticks)
            return new QueuePage([], null);

        await using var conn = await OpenConnectionAsync(ct);
        await using var command = new MySqlCommand(MySqlQueueSql.InspectList(_table, state), conn);
        command.Parameters.AddWithValue("@limit", limit + 1);
        if (state is QueueInspectionState.Pending or QueueInspectionState.InProgress)
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

        var messages = new List<QueueMessageInfo>(limit);
        string? next = null;
        (long, long) lastKey = default;
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (messages.Count == limit)
            {
                next = CursorOf(state, lastKey);
                break;
            }
            lastKey = state is QueueInspectionState.Pending or QueueInspectionState.InProgress
                ? (reader.GetInt64(0), 0)
                : (reader.GetDateTime(0).Ticks, reader.GetInt64(1));
            messages.Add(ReadMessage(reader, state));
        }
        return new QueuePage(messages, next);
    }

    private static QueueMessageInfo ReadMessage(MySqlDataReader reader, QueueInspectionState state)
    {
        var id = reader.GetFieldValue<byte[]>(2);
        return new QueueMessageInfo(
            Id: new Guid(id).ToString(),
            State: state,
            Attempts: reader.GetInt32(6),
            CreatedAt: AsUtc(reader.GetDateTime(3)),
            DeliverAt: reader.IsDBNull(4) ? null : AsUtc(reader.GetDateTime(4)),
            FailedAt: reader.IsDBNull(5) ? null : AsUtc(reader.GetDateTime(5)),
            LastError: reader.IsDBNull(7) ? null : reader.GetString(7),
            Payload: reader.GetFieldValue<byte[]>(8));
    }

    private static DateTimeOffset AsUtc(DateTime value) => new(DateTime.SpecifyKind(value, DateTimeKind.Utc));

    private static string CursorOf(QueueInspectionState state, (long, long) key) =>
        state is QueueInspectionState.Pending or QueueInspectionState.InProgress
            ? key.Item1.ToString(CultureInfo.InvariantCulture)
            : string.Create(CultureInfo.InvariantCulture, $"{key.Item1}.{key.Item2}");

    private static (long, long) ParseCursor(QueueInspectionState state, string? cursor)
    {
        if (cursor is null) return (-1, -1);

        var parts = cursor.Split('.');
        var wanted = state is QueueInspectionState.Pending or QueueInspectionState.InProgress ? 1 : 2;
        long first, second = -1;
        if (parts.Length != wanted
            || !long.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out first)
            || (wanted == 2 && !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out second)))
            throw new ArgumentException($"'{cursor}' is not a valid cursor for {state} messages.", nameof(cursor));

        return (first, second);
    }
}
