namespace NymBroker.Core.Scheduling;

internal static class ScheduledActionName
{
    /// <summary>Longest name; the database stores size their key column for it.</summary>
    internal const int MaxLength = 200;

    internal static void Validate(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name.Length > MaxLength)
            throw new ArgumentException($"A scheduled action name can be at most {MaxLength} characters.", nameof(name));
        if (name.Any(char.IsControl))
            throw new ArgumentException("A scheduled action name cannot contain control characters.", nameof(name));
    }
}
