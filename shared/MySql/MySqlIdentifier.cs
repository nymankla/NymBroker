using System.Text.RegularExpressions;

namespace NymBroker.Shared;

internal static partial class MySqlIdentifier
{
    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_$]*$")]
    private static partial Regex Part();

    /// <summary>Validates and backtick-quotes <c>table</c> or <c>db.table</c>; throws <see cref="ArgumentException"/> otherwise.</summary>
    internal static string QuoteTableName(string tableName)
    {
        var parts = (tableName ?? "").Split('.');
        if (parts.Length > 2 || !parts.All(p => Part().IsMatch(p)))
            throw new ArgumentException(
                $"TableName '{tableName}' is not valid: use 'table' or 'database.table' with letters, digits, '_' and '$'.", nameof(tableName));
        return string.Join('.', parts.Select(p => $"`{p}`"));
    }
}
