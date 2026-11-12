using System.Data;
using System.Data.Common;
using System.Globalization;
using NymBroker.Core.Outbox;

namespace NymBroker.Shared;

/// <summary>
/// The SQL of one outbox table. Parameters: <c>@endpoint</c>, <c>@body</c>, <c>@deliverAt</c> (Insert); <c>@claimId</c>,
/// <c>@batchSize</c>, <c>@leaseSeconds</c> (Claim); <c>@id</c> and <c>@claimId</c> (the rest), plus <c>@delayMs</c> and
/// <c>@error</c> (RetryLater), <c>@error</c> (MarkFailed). Claim returns <c>id, endpoint_name, body, deliver_at_utc, attempts</c>.
/// </summary>
internal sealed record OutboxStatements(
    string Insert,
    string Claim,
    string MarkSent,
    string RetryLater,
    string MarkFailed,
    string Release);

/// <summary>How one database stores claim ids and times, and claims rows.</summary>
internal abstract class OutboxDialect(OutboxStatements sql)
{
    // Row states, inlined in every package's SQL.
    internal const int Pending = 1;
    internal const int Failed = 2;

    public OutboxStatements Sql { get; } = sql;

    public abstract void AddClaimId(DbCommand command, Guid claimId);

    public abstract void AddTime(DbCommand command, string name, DateTimeOffset? value);

    public abstract DateTimeOffset ReadTime(DbDataReader reader, int ordinal);

    /// <summary>Claims due rows in one statement (<see cref="OutboxStatements.Claim"/>); MySQL needs two round trips.</summary>
    public virtual async Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(
        DbConnection connection, Guid claimId, int maxCount, int leaseSeconds, CancellationToken ct)
    {
        await using var command = Command(connection, Sql.Claim);
        AddClaimId(command, claimId);
        Add(command, "@batchSize", DbType.Int32, maxCount);
        Add(command, "@leaseSeconds", DbType.Int32, leaseSeconds);
        return await ReadClaimedAsync(command, ct);
    }

    /// <summary>Reads the claimed rows from every result set, ordered by id.</summary>
    public async Task<IReadOnlyList<ClaimedOutboxMessage>> ReadClaimedAsync(DbCommand command, CancellationToken ct)
    {
        var rows = new List<ClaimedOutboxMessage>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        do
        {
            while (await reader.ReadAsync(ct))
                rows.Add(ReadRow(reader));
        } while (await reader.NextResultAsync(ct));

        rows.Sort(static (a, b) => a.Id.CompareTo(b.Id));
        return rows;
    }

    private ClaimedOutboxMessage ReadRow(DbDataReader reader) => new(
        Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
        reader.GetString(1),
        (byte[])reader.GetValue(2),
        reader.IsDBNull(3) ? null : ReadTime(reader, 3),
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));

    public static DbCommand Command(DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command;
    }

    public static void Add(DbCommand command, string name, DbType type, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
