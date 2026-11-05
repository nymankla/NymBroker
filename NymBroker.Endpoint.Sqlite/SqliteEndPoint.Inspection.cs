using System.Globalization;
using Microsoft.Data.Sqlite;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Endpoint.Sqlite;

// IQueueInspector: read-only queries on the same serialized connection as the queue itself.
public sealed partial class SqliteEndPoint
{
    public async Task<QueueCounts> GetCountsAsync(CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            var (conn, _) = await EnsureConnectionAsync(ct);
            await using var command = conn.CreateCommand();
            command.CommandText = SqliteQueueSql.InspectCounts(_settings.TableName);
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return new QueueCounts(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(4), reader.GetInt64(2), reader.GetInt64(3));
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
        DateTimeOffset? failedSince = null, CancellationToken ct = default)
    {
        if (state is < QueueInspectionState.Pending or > QueueInspectionState.Failed)
            throw new ArgumentOutOfRangeException(nameof(state), state, "Only pending, in progress, deferred and failed messages can be listed.");
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, IQueueInspector.MaxPageSize);

        var (after1, after2) = ParseCursor(state, cursor);
        var since = failedSince is { } f ? f.ToUnixTimeSeconds() : 0;

        await _dbLock.WaitAsync(ct);
        try
        {
            var (conn, _) = await EnsureConnectionAsync(ct);
            await using var command = conn.CreateCommand();
            command.CommandText = SqliteQueueSql.InspectList(_settings.TableName, state);
            command.Parameters.AddWithValue("$after1", after1);
            command.Parameters.AddWithValue("$after2", after2);
            command.Parameters.AddWithValue("$since", since);
            command.Parameters.AddWithValue("$limit", limit + 1);

            var messages = new List<QueueMessageInfo>(limit);
            string? next = null;
            (long, long) lastKey = default;
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                // One extra row tells whether another page exists; the cursor is the last row actually returned.
                if (messages.Count == limit)
                {
                    next = CursorOf(state, lastKey);
                    break;
                }
                lastKey = (reader.GetInt64(0), reader.GetInt64(1));
                messages.Add(ReadMessage(reader, state));
            }
            return new QueuePage(messages, next);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    private static QueueMessageInfo ReadMessage(SqliteDataReader reader, QueueInspectionState state)
    {
        // Columns: key1, key2 (cursor), MessageId, CreatedAtUtc (s), DeliverAtUtc (ms), FailedAtUtc (s), AttemptCount, LastError, Payload
        var id = reader.GetFieldValue<byte[]>(2);
        return new QueueMessageInfo(
            Id: id.Length == 16 ? new Guid(id).ToString() : System.Text.Encoding.UTF8.GetString(id),
            State: state,
            Attempts: reader.GetInt32(6),
            CreatedAt: DateTimeOffset.FromUnixTimeSeconds(reader.GetInt64(3)),
            DeliverAt: reader.IsDBNull(4) ? null : DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(4)),
            FailedAt: reader.IsDBNull(5) ? null : DateTimeOffset.FromUnixTimeSeconds(reader.GetInt64(5)),
            LastError: reader.IsDBNull(7) ? null : reader.GetString(7),
            Payload: reader.GetFieldValue<byte[]>(8));
    }

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