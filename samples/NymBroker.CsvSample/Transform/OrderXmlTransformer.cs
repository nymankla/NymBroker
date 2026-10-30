using System.Text;
using System.Xml.Linq;
using NymBroker.Core.Serialize;
using NymBroker.Core.Transform;

namespace NymBroker.CsvSample.Transform;

/// <summary>
/// Writes each order sent to its endpoint as XML instead of the NymBroker JSON envelope —
/// e.g. for a legacy system that only reads <c>&lt;Order&gt;</c> documents.
/// </summary>
public sealed class OrderXmlTransformer : IOutputTransformer
{
    public byte[]? Transform(RawMessageContext message, string destinationEndpoint)
    {
        var order = message.RawMessage;
        var xml = new XElement("Order",
            new XAttribute("id", order.GetProperty("orderId").GetString()!),
            new XElement("Customer", order.GetProperty("customer").GetString()),
            new XElement("Amount", order.GetProperty("amount").GetDecimal()),
            new XElement("Priority", order.GetProperty("priority").GetString()));
        return Encoding.UTF8.GetBytes(xml.ToString(SaveOptions.DisableFormatting));
    }
}
