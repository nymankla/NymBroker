namespace MessageBroker.Core.Message;

public sealed class EndpointAddress
{
    public string? To { get; set; }
    public string? From { get; set; }

    public static EndpointAddress Create(string to, string? from = null) => new() { To = to, From = from };
}
