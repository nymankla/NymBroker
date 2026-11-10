using Microsoft.Data.Sqlite;

namespace NymBroker.Endpoint.Sqlite;

public sealed partial class SqliteEndPoint
{
    /// <summary>A statement that writes one envelope: <c>$messageId</c>, <c>$payload</c>.</summary>
    private sealed record InsertStatement(SqliteCommand Command, SqliteParameter MessageId, SqliteParameter Payload);

    /// <summary>A deferred insert: <c>$deliverAt</c> (Unix ms), <c>$messageId</c>, <c>$payload</c>.</summary>
    private sealed record InsertDeferredStatement(SqliteCommand Command, SqliteParameter DeliverAt, SqliteParameter MessageId, SqliteParameter Payload);

    /// <summary>Copying or deleting due deferred rows: <c>$now</c> (Unix ms), <c>$batchSize</c>.</summary>
    private sealed record DueStatement(SqliteCommand Command, SqliteParameter Now, SqliteParameter BatchSize);

    /// <summary>Claiming a batch: <c>$leaseSeconds</c>, <c>$batchSize</c>.</summary>
    private sealed record ClaimStatement(SqliteCommand Command, SqliteParameter LeaseSeconds, SqliteParameter BatchSize);

    /// <summary>Writing one outcome: <c>$id</c>, <c>$attempt</c>, and <c>$error</c> except for Completed.</summary>
    private sealed record MarkStatement(SqliteCommand Command, SqliteParameter Id, SqliteParameter Attempt, SqliteParameter? Error);

    /// <summary>The retention cleanup of one status: <c>$olderThanSeconds</c>, <c>$batchSize</c>.</summary>
    private sealed record DeleteFinishedStatement(SqliteCommand Command, SqliteParameter OlderThan, SqliteParameter BatchSize);

    /// <summary>Every statement prepared once on the endpoint's connection; only parameter values change per call.</summary>
    private sealed class PreparedCommands : IDisposable
    {
        public required InsertStatement Insert { get; init; }
        public required InsertDeferredStatement InsertDeferred { get; init; }
        public required DueStatement CopyDue { get; init; }
        public required DueStatement DeleteDue { get; init; }
        public required ClaimStatement Claim { get; init; }
        public required MarkStatement MarkCompleted { get; init; }
        public required MarkStatement MarkFailed { get; init; }
        public required MarkStatement MarkPending { get; init; }
        public required DeleteFinishedStatement DeleteCompleted { get; init; }
        public required DeleteFinishedStatement DeleteFailed { get; init; }

        public static PreparedCommands Create(SqliteConnection conn, string table)
        {
            var commands = new PreparedCommands
            {
                Insert = CreateInsert(Command(conn, SqliteQueueSql.Insert(table))),
                InsertDeferred = CreateInsertDeferred(Command(conn, SqliteQueueSql.InsertDeferred(table))),
                CopyDue = CreateDue(Command(conn, SqliteQueueSql.CopyDue(table))),
                DeleteDue = CreateDue(Command(conn, SqliteQueueSql.DeleteDue(table))),
                Claim = CreateClaim(Command(conn, SqliteQueueSql.Claim(table))),
                MarkCompleted = CreateMark(Command(conn, SqliteQueueSql.MarkCompleted(table)), withError: false),
                MarkFailed = CreateMark(Command(conn, SqliteQueueSql.MarkFailed(table)), withError: true),
                MarkPending = CreateMark(Command(conn, SqliteQueueSql.MarkPending(table)), withError: true),
                DeleteCompleted = CreateDeleteFinished(Command(conn, SqliteQueueSql.DeleteCompleted(table))),
                DeleteFailed = CreateDeleteFinished(Command(conn, SqliteQueueSql.DeleteFailed(table)))
            };

            // Not the deferred statements: a table created by hand may not have <table>_deferred yet, which must only
            // affect deferred messages. SQLite prepares them when they first run.
            foreach (var command in commands.All.Except([commands.InsertDeferred.Command, commands.CopyDue.Command, commands.DeleteDue.Command]))
                command.Prepare();
            return commands;
        }

        private IEnumerable<SqliteCommand> All =>
        [
            Insert.Command, InsertDeferred.Command, CopyDue.Command, DeleteDue.Command, Claim.Command,
            MarkCompleted.Command, MarkFailed.Command, MarkPending.Command, DeleteCompleted.Command, DeleteFailed.Command
        ];

        public void SetTransaction(SqliteTransaction? tx)
        {
            foreach (var command in new[] { Insert.Command, Claim.Command, MarkCompleted.Command, MarkFailed.Command, MarkPending.Command })
                command.Transaction = tx;
        }

        public void Dispose()
        {
            foreach (var command in All)
                command.Dispose();
        }

        private static InsertStatement CreateInsert(SqliteCommand command)
            => new(command, Parameter(command, "$messageId", SqliteType.Blob), Parameter(command, "$payload", SqliteType.Blob));

        private static InsertDeferredStatement CreateInsertDeferred(SqliteCommand command)
            => new(command, Parameter(command, "$deliverAt", SqliteType.Integer),
                Parameter(command, "$messageId", SqliteType.Blob), Parameter(command, "$payload", SqliteType.Blob));

        private static DueStatement CreateDue(SqliteCommand command)
            => new(command, Parameter(command, "$now", SqliteType.Integer), Parameter(command, "$batchSize", SqliteType.Integer));

        private static ClaimStatement CreateClaim(SqliteCommand command)
            => new(command, Parameter(command, "$leaseSeconds", SqliteType.Integer), Parameter(command, "$batchSize", SqliteType.Integer));

        private static MarkStatement CreateMark(SqliteCommand command, bool withError)
            => new(command, Parameter(command, "$id", SqliteType.Integer), Parameter(command, "$attempt", SqliteType.Integer),
                withError ? Parameter(command, "$error", SqliteType.Text) : null);

        private static DeleteFinishedStatement CreateDeleteFinished(SqliteCommand command)
            => new(command, Parameter(command, "$olderThanSeconds", SqliteType.Integer), Parameter(command, "$batchSize", SqliteType.Integer));

        private static SqliteCommand Command(SqliteConnection conn, string sql)
        {
            var command = conn.CreateCommand();
            command.CommandText = sql;
            return command;
        }

        private static SqliteParameter Parameter(SqliteCommand command, string name, SqliteType type)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.SqliteType = type;
            parameter.Value = DBNull.Value;
            command.Parameters.Add(parameter);
            return parameter;
        }
    }
}
