using System.Text;
using Microsoft.Data.Sqlite;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.Sqlite;

// IQueueReplayer: one guarded UPDATE per selected row, all in one BEGIN IMMEDIATE transaction on the endpoint's connection.
// Local statements are cheap, and a per-row statement uses the partial failed index directly.
public sealed partial class SqliteEndPoint
{
    public async Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        var items = QueueReplaySelection.Parse(messages);

        await _dbLock.WaitAsync(ct);
        try
        {
            var (conn, _) = await EnsureConnectionAsync(ct);
            await using var tx = BeginImmediate(conn);
            var replayed = await ReplayRowsAsync(conn, tx, items, ct);
            await tx.CommitAsync(ct);
            return QueueReplaySelection.Result(items, replayed);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    private async Task<HashSet<Guid>> ReplayRowsAsync(SqliteConnection conn, SqliteTransaction tx,
        List<QueueReplaySelection.Item> items, CancellationToken ct)
    {
        await using var command = conn.CreateCommand();
        command.Transaction = tx;
        command.CommandText = SqliteQueueSql.ReplayFailed(_settings.TableName);
        var id = command.Parameters.Add("$id", SqliteType.Blob);
        var legacyId = command.Parameters.Add("$legacyId", SqliteType.Blob);
        var failedAt = command.Parameters.Add("$failedAt", SqliteType.Integer);

        var replayed = new HashSet<Guid>();
        foreach (var item in items)
        {
            id.Value = item.Id.ToByteArray();
            legacyId.Value = Encoding.UTF8.GetBytes(item.Text);
            failedAt.Value = new DateTimeOffset(item.FailedAtUtc, TimeSpan.Zero).ToUnixTimeSeconds();
            if (await command.ExecuteNonQueryAsync(ct) > 0)
                replayed.Add(item.Id);
        }
        return replayed;
    }
}
