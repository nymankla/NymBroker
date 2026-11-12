// CA2208: the name reported is the settings property that is wrong, not this method's parameter.
#pragma warning disable CA2208

namespace NymBroker.Shared;

/// <summary>Checks shared by every outbox package's settings.</summary>
internal static class OutboxSettingsCheck
{
    internal static void RequireConnectionString(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("ConnectionString is required.", "ConnectionString");
    }
}
