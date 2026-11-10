using System.Data.Common;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

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
            return await QueueInspection.ReadCountsAsync(command, ct);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
        DateTimeOffset? failedSince = null, CancellationToken ct = default)
    {
        QueueInspection.ValidateListArguments(state, limit);
        var (after1, after2) = QueueInspection.ParseCursor(state, cursor);
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

            // Both cursor keys are integer columns here, also for the time-ordered states.
            return await QueueInspection.ReadPageAsync(command, state, limit,
                new QueueInspection.RowReader(r => (r.GetInt64(0), r.GetInt64(1)), r => ReadMessage(r, state)), ct);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    private static QueueMessageInfo ReadMessage(DbDataReader reader, QueueInspectionState state)
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
}
