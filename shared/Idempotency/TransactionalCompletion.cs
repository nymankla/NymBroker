using System.Data;
using System.Data.Common;

namespace NymBroker.Shared;

/// <summary>
/// <c>ITransactionalIdempotencyStore.CompleteInTransactionAsync</c> for every database store: runs the store's own
/// Complete statement (parameters <c>@id</c>, <c>@ttlSeconds</c>) on the caller's connection, in the caller's transaction.
/// </summary>
internal static class TransactionalCompletion
{
    /// <param name="id">The message id as the store's table holds it.</param>
    internal static async Task ExecuteAsync<TConnection>(
        DbTransaction transaction, string completeSql, (DbType Type, object Value) id, int ttlSeconds, CancellationToken ct)
        where TConnection : DbConnection
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction.Connection is not TConnection connection)
            throw new ArgumentException(transaction.Connection is null
                    ? "The transaction has already been committed or rolled back."
                    : $"The idempotency table is written in the caller's transaction, so it must be a {typeof(TConnection).Name} transaction.",
                nameof(transaction));

        await using var command = connection.CreateCommand();
        command.CommandText = completeSql;
        command.Transaction = transaction;
        Add(command, "@id", id.Type, id.Value);
        Add(command, "@ttlSeconds", DbType.Int32, ttlSeconds);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static void Add(DbCommand command, string name, DbType type, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
