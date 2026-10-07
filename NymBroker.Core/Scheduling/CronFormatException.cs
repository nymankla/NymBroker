namespace NymBroker.Core.Scheduling;

/// <summary>Thrown when a cron expression is not valid.</summary>
internal sealed class CronFormatException(string message) : FormatException(message);
