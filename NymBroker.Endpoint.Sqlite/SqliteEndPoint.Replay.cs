using System.Text;
using Microsoft.Data.Sqlite;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.Sqlite;

// IQueueReplayer and IQueueFailedRemover: one guarded UPDATE / DELETE per selected row, all in one BEGIN IMMEDIATE
// transaction on the endpoint's connection. Local statements are cheap, and a per-row statement uses the partial failed
// index directly.
public sealed partial class SqliteEndPoint
{
    public async Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);
        var replayed = await ChangeFailedAsync(SqliteQueueSql.ReplayFailed(_settings.TableName), items, ct);
        return QueueReplaySelection.Result(items, replayed);
    }

    public async Task<QueueRemoveResult> RemoveFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);
        var removed = await ChangeFailedAsync(SqliteQueueSql.RemoveFailed(_settings.TableName), items, ct);
        return QueueReplaySelection.RemoveResult(items, removed);
    }

    /// <summary>Runs <paramref name="sql"/> once per selected row in one transaction; returns the message ids it changed.</summary>
    private async Task<HashSet<Guid>> ChangeFailedAsync(string sql, List<QueueReplaySelection.Item> items, CancellationToken ct)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            var (conn, _) = await EnsureConnectionAsync(ct);
            await using var tx = BeginImmediate(conn);
            var changed = await ChangeRowsAsync(conn, tx, sql, items, ct);
            await tx.CommitAsync(ct);
            return changed;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    private static async Task<HashSet<Guid>> ChangeRowsAsync(SqliteConnection conn, SqliteTransaction tx, string sql,
        List<QueueReplaySelection.Item> items, CancellationToken ct)
    {
        await using var command = conn.CreateCommand();
        command.Transaction = tx;
        command.CommandText = sql;
        var id = command.Parameters.Add("$id", SqliteType.Blob);
        var legacyId = command.Parameters.Add("$legacyId", SqliteType.Blob);
        var failedAt = command.Parameters.Add("$failedAt", SqliteType.Integer);

        var changed = new HashSet<Guid>();
        foreach (var item in items)
        {
            id.Value = item.Id.ToByteArray();
            legacyId.Value = Encoding.UTF8.GetBytes(item.Text);
            failedAt.Value = new DateTimeOffset(item.FailedAtUtc, TimeSpan.Zero).ToUnixTimeSeconds();
            if (await command.ExecuteNonQueryAsync(ct) > 0)
                changed.Add(item.Id);
        }
        return changed;
    }
}
