namespace NymBroker.Shared;

internal static class SqlServerIdentifier
{
    internal static string QuoteQualified(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("SQL Server identifier cannot be empty.");

        return string.Join('.', identifier.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(QuoteSimple));
    }

    internal static string QuoteSimple(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("SQL Server identifier cannot be empty.");

        return $"[{identifier.Replace("]", "]]")}]";
    }

    /// <summary>Escapes a value for use inside an <c>N'...'</c> string literal.</summary>
    internal static string Literal(string value) => value.Replace("'", "''");
}
