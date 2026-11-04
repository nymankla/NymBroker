// CA2208: the name reported is the settings property that is wrong, not this method's parameter.
#pragma warning disable CA2208

namespace NymBroker.Shared;

internal static class SqliteStoreTable
{
    internal static string Quote(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("SQLite identifier cannot be empty.");

        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    internal static void Validate(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName) || tableName.Any(char.IsControl))
            throw new ArgumentException($"TableName '{tableName}' is not a valid SQLite table name.", "TableName");
    }
}
