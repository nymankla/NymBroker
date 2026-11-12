using System.Text;
using Npgsql;

// CA2208: the name reported is the settings property that is wrong, not this method's parameter.
#pragma warning disable CA2208

namespace NymBroker.Shared;

internal static class PostgresStoreSchema
{
    /// <summary>
    /// <c>CREATE … IF NOT EXISTS</c> is not safe against a concurrent CREATE (instances starting at the same time): the
    /// loser fails on a catalog unique index. A transaction-scoped advisory lock on the unqualified table name, parameter
    /// <c>lockKey</c> in <paramref name="createSchemaSql"/>, makes qualified and unqualified references coordinate.
    /// </summary>
    internal static async Task CreateAsync(NpgsqlDataSource dataSource, string createSchemaSql, string tableName, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using (var command = new NpgsqlCommand(createSchemaSql, connection, transaction))
        {
            var name = PostgresIdentifier.Split(tableName)[^1];
            command.Parameters.AddWithValue("lockKey", $"nymbroker:{name}");
            await command.ExecuteNonQueryAsync(ct);
        }
        await transaction.CommitAsync(ct);
    }

    internal static void ValidateTableName(string tableName, int maxParts = int.MaxValue)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("TableName is required.", "TableName");

        var parts = tableName.Split('.');
        if (parts.Length > maxParts ||
            parts.Any(string.IsNullOrWhiteSpace) ||
            parts.Any(part => Encoding.UTF8.GetByteCount(part.Trim()) > 63))
            throw new ArgumentException($"TableName '{tableName}' is not a valid PostgreSQL (optionally schema-qualified) table name.", "TableName");
    }
}
