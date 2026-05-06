namespace NymBroker.Core.Endpoint.File;

public sealed class FileSettings
{
    /// <summary>Directory to watch for incoming messages.</summary>
    public string ReadPath { get; set; } = "in";

    /// <summary>Directory to write outgoing messages.</summary>
    public string PostPath { get; set; } = "out";

    public string SearchPattern { get; set; } = "*.json";

    /// <summary>How long to wait between polls (used when FileSystemWatcher misses events).</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    public bool IsAbsolutePath { get; set; } = false;
}
