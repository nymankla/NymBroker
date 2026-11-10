using System.Data.Common;
using System.Globalization;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Shared;

/// <summary>
/// Provider-neutral parts of the SQL endpoints' <see cref="IQueueInspector"/>: argument checks, the cursor format,
/// counts and the limit + 1 page loop. The SQL and its parameter binding stay in each endpoint.
/// </summary>
/// <remarks>
/// Cursor format (clients may keep cursors, so it must not change): pending and in-progress states use the queue key
/// (<c>"key"</c>); deferred and failed use the timestamp in UTC ticks and the row id (<c>"ticks.id"</c>).
/// </remarks>
internal static class QueueInspection
{
    /// <summary>Pending and in-progress pages are keyed by one id; deferred and failed by a timestamp and an id.</summary>
    internal static bool IsKeyedById(QueueInspectionState state)
        => state is QueueInspectionState.Pending or QueueInspectionState.InProgress;

    internal static void ValidateListArguments(QueueInspectionState state, int limit)
    {
        if (state is < QueueInspectionState.Pending or > QueueInspectionState.Failed)
            throw new ArgumentOutOfRangeException(nameof(state), state, "Only pending, in progress, deferred and failed messages can be listed.");
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, IQueueInspector.MaxPageSize);
    }

    /// <summary>Reads one row whose columns are the counts for pending, in progress, failed, completed and deferred.</summary>
    internal static async Task<QueueCounts> ReadCountsAsync(DbCommand command, CancellationToken ct)
    {
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new QueueCounts(
            Pending: reader.GetInt64(0), InProgress: reader.GetInt64(1), Deferred: reader.GetInt64(4),
            Failed: reader.GetInt64(2), Completed: reader.GetInt64(3));
    }

    /// <summary>
    /// Runs <paramref name="command"/> (which asks for <paramref name="limit"/> + 1 rows) and builds the page: the extra
    /// row tells whether another page exists, and the cursor is the key of the last row actually returned.
    /// </summary>
    internal static async Task<QueuePage> ReadPageAsync(DbCommand command, QueueInspectionState state, int limit, RowReader read,
        CancellationToken ct)
    {
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
            lastKey = read.Key(reader);
            messages.Add(read.Message(reader));
        }
        return new QueuePage(messages, next);
    }

    /// <summary>How one listed row is read: its cursor key, and the message.</summary>
    internal sealed record RowReader(Func<DbDataReader, (long, long)> Key, Func<DbDataReader, QueueMessageInfo> Message);

    /// <summary>Key of a row whose first column is the queue id (pending, in progress) or a timestamp followed by the id.</summary>
    internal static (long, long) ReadTimestampKey(DbDataReader reader, QueueInspectionState state)
        => IsKeyedById(state)
            ? (reader.GetInt64(0), 0)
            : (reader.GetDateTime(0).Ticks, reader.GetInt64(1));

    /// <summary>
    /// Reads a row with columns key1, key2, MessageId, CreatedAt, DeliverAt, FailedAt, AttemptCount, LastError, Payload,
    /// where the three times are UTC <see cref="DateTime"/>s.
    /// </summary>
    internal static QueueMessageInfo ReadMessage(DbDataReader reader, QueueInspectionState state, string id)
        => new(
            Id: id,
            State: state,
            Attempts: reader.GetInt32(6),
            CreatedAt: AsUtc(reader.GetDateTime(3)),
            DeliverAt: reader.IsDBNull(4) ? null : AsUtc(reader.GetDateTime(4)),
            FailedAt: reader.IsDBNull(5) ? null : AsUtc(reader.GetDateTime(5)),
            LastError: reader.IsDBNull(7) ? null : reader.GetString(7),
            Payload: reader.GetFieldValue<byte[]>(8));

    internal static DateTimeOffset AsUtc(DateTime value) => new(DateTime.SpecifyKind(value, DateTimeKind.Utc));

    internal static string CursorOf(QueueInspectionState state, (long, long) key)
        => IsKeyedById(state)
            ? key.Item1.ToString(CultureInfo.InvariantCulture)
            : string.Create(CultureInfo.InvariantCulture, $"{key.Item1}.{key.Item2}");

    /// <summary>
    /// Parses a cursor into its two keys; <c>(-1, -1)</c> without a cursor, and the second key is -1 for a one-key cursor.
    /// With <paramref name="ticksMustBeDateTime"/>, a two-key cursor whose first key is past <see cref="DateTime.MaxValue"/> is rejected.
    /// </summary>
    internal static (long, long) ParseCursor(QueueInspectionState state, string? cursor, bool ticksMustBeDateTime = false)
    {
        if (cursor is null) return (-1, -1);

        var parts = cursor.Split('.');
        var wanted = IsKeyedById(state) ? 1 : 2;
        long first, second = -1;
        if (parts.Length != wanted
            || !long.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out first)
            || (wanted == 2 && !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out second))
            || (wanted == 2 && ticksMustBeDateTime && first > DateTime.MaxValue.Ticks))
            throw new ArgumentException($"'{cursor}' is not a valid cursor for {state} messages.", nameof(cursor));

        return (first, second);
    }
}
