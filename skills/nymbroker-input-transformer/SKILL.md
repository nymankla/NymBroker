---
name: nymbroker-input-transformer
description: Make a NymBroker endpoint accept data that is not a NymBroker JSON envelope — CSV, XML, fixed-width, plain text, or another system's JSON — by writing an IInputTransformer that turns raw bytes into a typed message for normal consumers and routes. Use when the user wants to read/import files, integrate a legacy or third-party system, parse CSV/XML/foreign JSON, or receive messages not produced by NymBroker.
---

# NymBroker input transformer

Normally an endpoint expects NymBroker's JSON envelope. An **input transformer** replaces that deserialization for one endpoint (or all): it gets the raw bytes and returns a `RawMessageContext` (type name + JSON payload). From there the message flows through expiry, filters, routes and consumers like any other — consumers receive a typed message.

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/pipeline-extensions.md#input-transformers

## 1. Clarify

- **Input format** — get a real sample (a few lines/records, including a bad one) from the user or the repo. Encoding (UTF-8? Windows-1252?), separators, header line, decimal/date formats.
- **Where it arrives** — usually a File endpoint (`ReadPath`, `SearchPattern` e.g. `*.csv`), or a queue another system writes to.
- **One input = one message?** A transformer returns **one** message per received item (one file, one queue message). For a file with many records, either have the producer send one record per item, or map the whole file to one message containing a list and let the consumer iterate.
- **Bad input** — drop silently, log and drop, or fail (retry / dead-letter)?
- The target message type, created with the **nymbroker-message** skill (it needs a `[MessageName]`, which the transformer must use).

## 2. Write the transformer

```csharp
using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Serialize;   // RawMessageContext
using NymBroker.Core.Transform;   // IInputTransformer

public sealed class CsvOrderTransformer(ILogger<CsvOrderTransformer> logger) : IInputTransformer
{
    public RawMessageContext? Transform(ReadOnlySpan<byte> input, string? sourceEndpoint)
    {
        var line  = Encoding.UTF8.GetString(input).Trim();
        var parts = line.Split(',');
        if (parts.Length != 4 || !decimal.TryParse(parts[2], NumberStyles.Number, CultureInfo.InvariantCulture, out var amount))
        {
            logger.LogWarning("Dropping invalid order line from {Endpoint}: {Line}", sourceEndpoint, line);
            return null;                                   // dropped, completed — not retried
        }

        return new RawMessageContext
        {
            Id            = Guid.NewGuid(),                // or a stable id derived from the input, for duplicate detection
            CorrelationId = Guid.NewGuid(),
            MessageType   = "orders.order-created",        // must equal the target type's [MessageName]
            Created       = DateTime.UtcNow,
            RawMessage    = JsonSerializer.SerializeToElement(new
            {
                orderId    = parts[0],
                customerId = parts[1],
                amount,
                priority   = parts[3]
            })
        };
    }
}
```

Rules:

- `MessageType` must match the `[MessageName]` of a type that has a consumer, route or topic — otherwise the message is logged as unresolved and dropped.
- `RawMessage` must deserialize into the target type: property names camelCase (or matching the record's parameters), compatible value types. Simplest: build the typed message and call `JsonSerializer.SerializeToElement(message, JsonSerializerOptions.Web)`.
- **Return `null`** to drop the input (completed, nothing logged by NymBroker — log yourself). **Throw** to fail it: transports that retry will retry, then dead-letter; Memory/File go to the broker's dead-letter endpoint.
- **Ids**: `Guid.Empty` skips duplicate detection. If the same input can arrive twice (file re-dropped, redelivery) and you use `AddIdempotentReceiver`, derive a deterministic id from the content or a business key (e.g. a GUID from a hash of the record).
- Keep it pure and fast: parse only. No database calls or HTTP — do that in the consumer, where failures are retried.
- `sourceEndpoint` lets one transformer handle several formats by endpoint.
- Don't let the transformer allocate huge strings for big files; stream-parse if inputs can be large.

## 3. Register

```csharp
builder.Services.AddNymBroker()
    .AddFileEndPoint("CsvInbox", new FileSettings { ReadPath = "csv-in", SearchPattern = "*.csv" })
    .AddInputTransformer<CsvOrderTransformer>("CsvInbox")   // omit the name for a fallback used by every endpoint
    .AddConsumer<OrderCreatedConsumer>()
    .Build();
```

- A File endpoint's default `SearchPattern` is `*.json` — set it for other extensions.
- An endpoint with a transformer only accepts the foreign format; NymBroker envelopes posted to it go through the transformer too. Use a separate endpoint for normal messages.
- To push raw bytes from code (tests, an HTTP webhook): `await broker.PostAsync("CsvInbox", stream)` with a `Stream`.

## 4. Test

Unit-test `Transform` directly with sample inputs (valid, invalid, edge cases such as decimal commas and empty lines): assert `MessageType` and that `RawMessage.Deserialize<OrderCreated>(JsonSerializerOptions.Web)` gives the expected values. Add one integration test that posts a raw line to a Memory endpoint with the transformer and waits for the consumer (nymbroker-testing skill).

## 5. Finish

Build and run the tests; tell the user how bad input is handled and where to drop files or send data.
