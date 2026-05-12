using System.Text;
using System.Text.Json;
using NymBroker.Core.Serialize;
using NymBroker.Core.Transform;

namespace NymBroker.CsvSample.Transform;

/// <summary>
/// Parses a single CSV line — orderId,customer,amount,priority — into a RawMessageContext
/// with messageType "order.created". Lines that don't have exactly 4 fields are dropped.
/// </summary>
public sealed class CsvOrderTransformer : IInputTransformer
{
    public RawMessageContext? Transform(ReadOnlySpan<byte> input, string? sourceEndpoint)
    {
        var line = Encoding.UTF8.GetString(input).Trim();
        var parts = line.Split(',');
        if (parts.Length != 4)
            return null;

        var orderId  = parts[0].Trim();
        var customer = parts[1].Trim();
        var priority = parts[3].Trim();

        if (!decimal.TryParse(parts[2].Trim(), System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var amount))
            return null;

        var json = JsonSerializer.SerializeToElement(new
        {
            orderId,
            customer,
            amount,
            priority
        });

        return new RawMessageContext
        {
            Id          = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            MessageType = "order.created",
            Created     = DateTime.UtcNow,
            RawMessage  = json
        };
    }
}
