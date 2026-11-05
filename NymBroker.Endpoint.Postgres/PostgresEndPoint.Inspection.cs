using System.Globalization;
using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Endpoint.Postgres;

public sealed partial class PostgresEndPoint
{
    public async Task<QueueCounts> GetCountsAsync(CancellationToken ct = default)
    {
        await using var conn = await OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresQueueSql.InspectCounts(_settings.TableName);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new QueueCounts(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4));
    }

    public async Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
        DateTimeOffset? failedSince = null, CancellationToken ct = default)
    {
        if (state is < QueueInspectionState.Pending or > QueueInspectionState.Failed)
            throw new ArgumentOutOfRangeException(nameof(state), state, "Only pending, in progress, deferred and failed messages can be listed.");
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, IQueueInspector.MaxPageSize);

        var (afterTime, afterId) = ParseCursor(state, cursor);
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

        var messages = new List<QueueMessageInfo>(limit);
        string? next = null;
        (DateTimeOffset? Time, long Id) lastKey = default;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (messages.Count == limit)
            {
                next = CursorOf(state, lastKey.Time, lastKey.Id);
                break;
            }

            lastKey = state is QueueInspectionState.Pending or QueueInspectionState.InProgress
                ? (null, reader.GetInt64(0))
                : (new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc)), reader.GetInt64(1));
            messages.Add(ReadMessage(reader, state));
        }
        return new QueuePage(messages, next);
    }

    private static QueueMessageInfo ReadMessage(NpgsqlDataReader reader, QueueInspectionState state)
        => new(
            Id: reader.GetGuid(2).ToString(),
            State: state,
            Attempts: reader.GetInt32(6),
            CreatedAt: new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc)),
            DeliverAt: reader.IsDBNull(4) ? null : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc)),
            FailedAt: reader.IsDBNull(5) ? null : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc)),
            LastError: reader.IsDBNull(7) ? null : reader.GetString(7),
            Payload: reader.GetFieldValue<byte[]>(8));

    private static string CursorOf(QueueInspectionState state, DateTimeOffset? time, long id)
        => state is QueueInspectionState.Pending or QueueInspectionState.InProgress
            ? id.ToString(CultureInfo.InvariantCulture)
            : string.Create(CultureInfo.InvariantCulture, $"{time!.Value.UtcTicks}.{id}");

    private static (DateTimeOffset? Time, long Id) ParseCursor(QueueInspectionState state, string? cursor)
    {
        if (cursor is null) return (null, -1);

        var parts = cursor.Split('.');
        var wanted = state is QueueInspectionState.Pending or QueueInspectionState.InProgress ? 1 : 2;
        long first, second = -1;
        if (parts.Length != wanted
            || !long.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out first)
            || (wanted == 2 && !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out second)))
            throw new ArgumentException($"'{cursor}' is not a valid cursor for {state} messages.", nameof(cursor));

        if (wanted == 1) return (null, first);
        try
        {
            return (new DateTimeOffset(first, TimeSpan.Zero), second);
        }
        catch (ArgumentOutOfRangeException)
        {
            throw new ArgumentException($"'{cursor}' is not a valid cursor for {state} messages.", nameof(cursor));
        }
    }
}
