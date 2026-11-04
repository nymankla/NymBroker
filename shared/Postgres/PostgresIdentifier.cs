namespace NymBroker.Shared;

internal static class PostgresIdentifier
{
    internal static string QuoteQualified(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("PostgreSQL identifier cannot be empty.");

        return string.Join('.', Split(identifier).Select(QuoteSimple));
    }

    internal static string QuoteSimple(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("PostgreSQL identifier cannot be empty.");

        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    internal static string[] Split(string identifier)
        => identifier.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}
