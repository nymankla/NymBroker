using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Core.Transform;
using NymBroker.Endpoint.Http;
using NymBroker.Endpoint.Sqlite;

// Webhooks with NymBroker, both directions, in one app:
//  - sending:   an HTTP endpoint ("PartnerWebhook") POSTs messages to a URL, signed with HMAC-SHA256;
//  - receiving: POST /hooks/orders verifies the signature, stores the body in a durable SQLite queue and answers 202;
//               POST /hooks/shop takes another system's JSON and an input transformer turns it into a NymBroker message.
// The consumer runs later, from the queue — never inside the HTTP request.
//
//   dotnet run --project samples/NymBroker.WebhookSample            # run the web app on http://localhost:5187
//   dotnet run --project samples/NymBroker.WebhookSample -- --demo  # send, receive and check, then exit 0 or 1
//
// Here the sender and the receiver are the same app so the sample is self-contained; normally they are two systems.

const int Port = 5187;
const string SigningSecret = "whsec_sample_only_use_a_secret_store";   // sample only: load it from a secret store
const string ShopApiKey = "shop_key_sample_only";
const int MaxBodyBytes = 1024 * 1024;
var demo = args.Contains("--demo");
const string InboxDb = "webhook-inbox.db";

if (demo)
    foreach (var file in new[] { InboxDb, $"{InboxDb}-wal", $"{InboxDb}-shm" })
        File.Delete(file);

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://localhost:{Port}");
builder.Logging.SetMinimumLevel(LogLevel.Warning)
    .AddFilter("OrderConsumer", LogLevel.Information)
    .AddFilter("NymBroker.Core.Impl.NymBrokerImpl", LogLevel.Debug);   // shows "Dropping duplicate message …"

builder.Services.AddNymBroker()
    // Receiving side: durable queues the HTTP handlers write to. A consumer failure is retried from the table.
    .AddSqliteEndPoint("Inbox", new SqliteSettings { ConnectionString = $"Data Source={InboxDb}", TableName = "OrderInbox", PollInterval = TimeSpan.FromMilliseconds(50) })
    .AddSqliteEndPoint("ShopInbox", new SqliteSettings { ConnectionString = $"Data Source={InboxDb}", TableName = "ShopInbox", PollInterval = TimeSpan.FromMilliseconds(50) })
    .AddInputTransformer<ShopOrderTransformer>("ShopInbox")
    // Senders retry, so the same message can arrive twice: drop repeats by message id (the Idempotency-Key header).
    .AddIdempotentReceiver(TimeSpan.FromHours(24))
    .AddConsumer<OrderConsumer>()
    // Sending side: every message posted to "PartnerWebhook" becomes a signed POST.
    .AddHttpEndPoint("PartnerWebhook", new HttpSettings
    {
        Url = $"http://localhost:{Port}/hooks/orders",   // https in production (docs/security.md)
        SigningSecret = SigningSecret,
        MaxRetryAttempts = 3
    })
    .Build();

var app = builder.Build();

// Receives NymBroker envelopes from another NymBroker instance (here: ourselves), signed with the shared secret.
app.MapPost("/hooks/orders", async (HttpRequest request, INymBroker broker, CancellationToken ct) =>
{
    var body = await ReadBodyAsync(request, ct);
    if (body is null) return Results.StatusCode(StatusCodes.Status413PayloadTooLarge);
    if (!WebhookSignature.IsValid(request.Headers["X-NymBroker-Signature"], body, SigningSecret, DateTimeOffset.UtcNow))
        return Results.Unauthorized();

    await broker.PostRawAsync("Inbox", body, ct);   // durable first; the consumer runs from the queue
    return Results.Accepted();
});

// Receives another system's JSON (a shop platform), authenticated with an API key; the input transformer on
// "ShopInbox" maps it to OrderReceived.
app.MapPost("/hooks/shop", async (HttpRequest request, INymBroker broker, CancellationToken ct) =>
{
    if (!CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(request.Headers["X-Shop-Key"].ToString()), Encoding.UTF8.GetBytes(ShopApiKey)))
        return Results.Unauthorized();
    var body = await ReadBodyAsync(request, ct);
    if (body is null) return Results.StatusCode(StatusCodes.Status413PayloadTooLarge);

    await broker.PostRawAsync("ShopInbox", body, ct);
    return Results.Accepted();
});

if (!demo)
{
    Console.WriteLine($"Webhook sample on http://localhost:{Port}: POST /hooks/orders (signed NymBroker envelopes), POST /hooks/shop (X-Shop-Key + shop JSON).");
    Console.WriteLine("Run with --demo to send, receive and check automatically.");
    await app.RunAsync();
    return 0;
}

await app.StartAsync();
var nymBroker = app.Services.GetRequiredService<INymBroker>();
using var http = new HttpClient { BaseAddress = new Uri($"http://localhost:{Port}") };
var failures = 0;
void Check(bool ok, string what) { Console.WriteLine($"  [{(ok ? "ok" : "FAIL")}] {what}"); if (!ok) failures++; }

// --- 1. Send two orders through the HTTP endpoint: signed POST → 202 → durable inbox → consumer ----------------------
Console.WriteLine("1. Two orders sent through the signed HTTP endpoint");
await nymBroker.PostAsync("PartnerWebhook", new OrderReceived("W-1", "alice", 10m));
await nymBroker.PostAsync("PartnerWebhook", new OrderReceived("W-2", "bob", 20m));
await WaitUntilAsync(() => OrderConsumer.Received.Count >= 2);
Check(OrderConsumer.Received.Select(o => o.OrderId).Order().SequenceEqual(["W-1", "W-2"]), "W-1 and W-2 arrived over HTTP and were consumed from the inbox queue");

// --- 2. The same envelope delivered twice (a sender retry after a lost response) is consumed once ------------------
Console.WriteLine("2. The same envelope sent twice");
var envelope = Envelope(Guid.NewGuid(), new OrderReceived("W-3", "carol", 30m));
await nymBroker.PostRawAsync("PartnerWebhook", envelope);
await nymBroker.PostRawAsync("PartnerWebhook", envelope);
await WaitUntilAsync(() => OrderConsumer.Received.Any(o => o.OrderId == "W-3"));
await Task.Delay(1000);   // give the duplicate the chance to show up
Check(OrderConsumer.Received.Count(o => o.OrderId == "W-3") == 1, "W-3 was delivered twice and consumed once (Idempotency-Key = message id)");

// --- 3. Requests that are not signed with the secret are rejected before anything is stored ------------------------
Console.WriteLine("3. Forged and unsigned requests");
var forged = Envelope(Guid.NewGuid(), new OrderReceived("EVIL-1", "mallory", 1_000_000m));
var unsigned = await http.PostAsync("/hooks/orders", new ByteArrayContent(forged));
var badSignature = new HttpRequestMessage(HttpMethod.Post, "/hooks/orders") { Content = new ByteArrayContent(forged) };
badSignature.Headers.Add("X-NymBroker-Signature", WebhookSignature.Sign("wrong-secret", DateTimeOffset.UtcNow.ToUnixTimeSeconds(), forged));
var forgedResponse = await http.SendAsync(badSignature);
var replay = new HttpRequestMessage(HttpMethod.Post, "/hooks/orders") { Content = new ByteArrayContent(forged) };
replay.Headers.Add("X-NymBroker-Signature", WebhookSignature.Sign(SigningSecret, DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeSeconds(), forged));
var replayResponse = await http.SendAsync(replay);
Check(unsigned.StatusCode == System.Net.HttpStatusCode.Unauthorized && forgedResponse.StatusCode == System.Net.HttpStatusCode.Unauthorized,
      "no signature and a wrong secret → 401");
Check(replayResponse.StatusCode == System.Net.HttpStatusCode.Unauthorized, "a correctly signed but hour-old request (a replay) → 401");

// --- 4. Another system's JSON → input transformer → the same consumer ---------------------------------------------
Console.WriteLine("4. A shop platform's own JSON format");
var shop = new HttpRequestMessage(HttpMethod.Post, "/hooks/shop")
{
    Content = new StringContent("""{"order_no":"S-100","total_cents":4990,"buyer":{"email":"dave@example.com"}}""", Encoding.UTF8, "application/json")
};
shop.Headers.Add("X-Shop-Key", ShopApiKey);
var shopResponse = await http.SendAsync(shop);
await WaitUntilAsync(() => OrderConsumer.Received.Any(o => o.OrderId == "S-100"));
var s100 = OrderConsumer.Received.Single(o => o.OrderId == "S-100");
Check(shopResponse.StatusCode == System.Net.HttpStatusCode.Accepted && s100 is { Customer: "dave@example.com", Amount: 49.90m },
      $"S-100 mapped by the input transformer: {s100.Customer}, {s100.Amount}");
Check(!OrderConsumer.Received.Any(o => o.OrderId == "EVIL-1"), "nothing from the rejected requests was consumed");

await app.StopAsync();
Console.WriteLine(failures == 0 ? "Demo passed." : $"Demo failed: {failures} check(s).");
return failures == 0 ? 0 : 1;

// --- helpers --------------------------------------------------------------------------------------------------------

// Reads the body, at most MaxBodyBytes (null if larger): never buffer an unbounded request from the internet.
static async Task<byte[]?> ReadBodyAsync(HttpRequest request, CancellationToken ct)
{
    if (request.ContentLength > MaxBodyBytes) return null;
    using var buffer = new MemoryStream();
    var chunk = new byte[16 * 1024];
    int read;
    while ((read = await request.Body.ReadAsync(chunk, ct)) > 0)
    {
        if (buffer.Length + read > MaxBodyBytes) return null;
        buffer.Write(chunk, 0, read);
    }
    return buffer.ToArray();
}

static async Task WaitUntilAsync(Func<bool> condition)
{
    var deadline = DateTime.UtcNow.AddSeconds(15);
    while (!condition())
    {
        if (DateTime.UtcNow > deadline) throw new TimeoutException("The expected state was not reached in 15 s.");
        await Task.Delay(50);
    }
}

// A NymBroker envelope with a fixed id, so two posts look like one message delivered twice.
static byte[] Envelope(Guid id, OrderReceived order) => Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
{
    id,
    correlationId = Guid.NewGuid(),
    messageType = "orders.received",
    created = DateTime.UtcNow,
    message = order
}, JsonSerializerOptions.Web));

[MessageName("orders.received")]
public sealed record OrderReceived(string OrderId = "", string Customer = "", decimal Amount = 0m);

public sealed class OrderConsumer(ILogger<OrderConsumer> logger) : IConsume<OrderReceived>
{
    public static readonly ConcurrentQueue<OrderReceived> Received = new();

    public Task ConsumeAsync(OrderReceived order, IMessageContext ctx, CancellationToken ct = default)
    {
        Received.Enqueue(order);
        logger.LogInformation("Order {Id} from {Customer} ({Amount}) received via {Endpoint}", order.OrderId, order.Customer, order.Amount, ctx.Address?.From);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Verifies <c>X-NymBroker-Signature: t=&lt;unix seconds&gt;,v1=&lt;hex HMAC-SHA256(secret, "&lt;t&gt;." + body)&gt;</c>, the format
/// <c>HttpSettings.SigningSecret</c> produces. Rejects timestamps more than five minutes off (replays) and compares in
/// constant time.
/// </summary>
public static class WebhookSignature
{
    public static readonly TimeSpan Tolerance = TimeSpan.FromMinutes(5);

    public static bool IsValid(string? header, byte[] body, string secret, DateTimeOffset now)
    {
        if (string.IsNullOrEmpty(header)) return false;
        string? t = null, v1 = null;
        foreach (var part in header.Split(','))
        {
            var kv = part.Split('=', 2);
            if (kv.Length != 2) continue;
            if (kv[0] == "t") t = kv[1];
            else if (kv[0] == "v1") v1 = kv[1];
        }
        if (!long.TryParse(t, out var timestamp) || v1 is null) return false;
        if ((now - DateTimeOffset.FromUnixTimeSeconds(timestamp)).Duration() > Tolerance) return false;

        var expected = Sign(secret, timestamp, body);
        return CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(expected), Encoding.ASCII.GetBytes($"t={t},v1={v1}"));
    }

    public static string Sign(string secret, long timestamp, byte[] body)
    {
        var mac = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes($"{timestamp}.").Concat(body).ToArray());
        return $"t={timestamp},v1={Convert.ToHexStringLower(mac)}";
    }
}

/// <summary>Maps the shop platform's JSON to <see cref="OrderReceived"/>.</summary>
public sealed class ShopOrderTransformer(ILogger<ShopOrderTransformer> logger) : IInputTransformer
{
    public RawMessageContext? Transform(ReadOnlySpan<byte> input, string? sourceEndpoint)
    {
        try
        {
            using var doc = JsonDocument.Parse(input.ToArray());
            var root = doc.RootElement;
            var orderNo = root.GetProperty("order_no").GetString()!;
            var order = new OrderReceived(orderNo, root.GetProperty("buyer").GetProperty("email").GetString() ?? "",
                root.GetProperty("total_cents").GetInt64() / 100m);
            return new RawMessageContext
            {
                // Deterministic id from the shop's order number: the shop's own retries are dropped as duplicates.
                Id = new Guid(SHA256.HashData(Encoding.UTF8.GetBytes($"shop:{orderNo}"))[..16]),
                CorrelationId = Guid.NewGuid(),
                MessageType = "orders.received",
                Created = DateTime.UtcNow,
                RawMessage = JsonSerializer.SerializeToElement(order, JsonSerializerOptions.Web)
            };
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException or FormatException)
        {
            logger.LogWarning("Dropping a shop webhook that is not a valid order: {Error}", ex.Message);
            return null;
        }
    }
}
