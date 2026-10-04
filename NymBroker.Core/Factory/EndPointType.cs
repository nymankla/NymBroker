namespace NymBroker.Core.Factory;

/// <summary>
/// Well-known values for <see cref="Configuration.EndPointConfiguration.Type"/>.
/// The type is an open string: extension packages define their own names (e.g. <c>"SqlServer"</c>)
/// and pick their entries up in a <c>With*()</c> builder extension, so config files can declare
/// endpoint types that Core does not know about. Matching is case-insensitive.
/// </summary>
public static class EndPointType
{
    public const string File     = "File";
    public const string RabbitMq = "RabbitMq";
    public const string Memory   = "Memory";
    public const string Sql      = "Sql";
    public const string Postgres = "Postgres";

    /// <summary>Endpoint types shipped with NymBroker (Core and the official add-on packages).</summary>
    public static IReadOnlyList<string> BuiltIn { get; } = [File, RabbitMq, Memory, Sql, Postgres];
}
