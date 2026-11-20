using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Core.Message;
using NymBroker.Core.Protection;
using NymBroker.Endpoint.Sqlite;

// Payload protection (#135) on a SQLite queue shared by a shop (sender) and billing (receiver). Self-checking: exits 0
// when every expectation holds, 1 otherwise.
//
//   dotnet run --project samples/NymBroker.PayloadProtectionSample
//
// The queue is filled first, then billing starts:
//  1. The shop sends a payment encrypted with key "2026-10". The row in the table holds ciphertext, while the id,
//     type and timestamps stay readable.
//  2. Key rotation: the shop now encrypts with "2026-11". Billing has both keys, so it reads both payments.
//  3. A legacy sender without protection posts plaintext. Billing requires protection on its queue, so the message is
//     dead-lettered as PayloadNotProtected.
//  4. A sender with a key billing doesn't have: dead-lettered as PayloadUnprotectFailed.
//  5. Someone with database access edits an encrypted payload: AES-GCM detects it, also PayloadUnprotectFailed.
// Dead-lettered protected messages stay encrypted in the table.

const string Card = "4111 1111 1111 1111";

var folder = Directory.CreateTempSubdirectory("nymbroker-protection-");
var db = Path.Combine(folder.FullName, "payments.db");
SqliteSettings Queue() => new() { ConnectionString = $"Data Source={db}", TableName = "Payments", PollInterval = TimeSpan.FromMilliseconds(50) };

// In production the keys come from a secret store (Key Vault, environment, user secrets), never from source code.
var october = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
var november = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
var stolen = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));

AesGcmPayloadProtector KeyRing(string current, params (string Id, string Key)[] keys)
{
    var options = new AesGcmPayloadProtectorOptions { CurrentKeyId = current };
    foreach (var (id, key) in keys) options.AddKeyFromBase64(id, key);
    return new AesGcmPayloadProtector(options);
}

var failures = new List<string>();
void Check(bool condition, string expectation)
{
    Console.WriteLine($"  {(condition ? "ok  " : "FAIL")} {expectation}");
    if (!condition) failures.Add(expectation);
}

// --- The senders post while billing is down ---
Console.WriteLine("Senders post to the Payments queue");
await PostAsync(KeyRing("2026-10", ("2026-10", october)), new PaymentRequested("ORD-001", Card, 120m));
await PostAsync(KeyRing("2026-11", ("2026-10", october), ("2026-11", november)), new PaymentRequested("ORD-002", Card, 80m));
await PostAsync(protector: null, new PaymentRequested("ORD-003", Card, 15m));                       // legacy, plaintext
await PostAsync(KeyRing("stolen", ("stolen", stolen)), new PaymentRequested("ORD-004", Card, 9999m)); // unknown key
await PostAsync(KeyRing("2026-11", ("2026-11", november)), new PaymentRequested("ORD-005", Card, 42m));
await TamperWithLastRowAsync();                                                                       // ORD-005 edited

var queued = await ReadRowsAsync(QueueInspectionState.Pending);
Console.WriteLine();
Console.WriteLine("1. What the table holds");
Console.WriteLine($"  {Shorten(queued[0])}");
Check(queued.Count == 5, $"5 messages are waiting ({queued.Count})");
Check(queued.Where((_, i) => i != 2).All(p => !p.Contains(Card) && p.Contains("\"protection\"")),
    "the 4 protected rows hold no card number, only ciphertext");
Check(queued[0].Contains("\"kid\":\"2026-10\"") && queued[1].Contains("\"kid\":\"2026-11\""), "each names its key id (rotation)");
Check(queued[0].Contains("\"messageType\":\"sample.protection.payment-requested\""), "the message type stays readable");

// --- Billing starts and processes the queue ---
var ledger = new Ledger();
var builder = Host.CreateApplicationBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning)
    .AddFilter("NymBroker.Core.Impl.NymBrokerImpl", LogLevel.Error)   // shows each rejected message
    .AddFilter("NymBroker.Endpoint", LogLevel.None);
builder.Logging.AddSimpleConsole(o => o.SingleLine = true);
builder.Services.AddSingleton(ledger);
builder.Services.AddNymBroker()
    .AddSqliteEndPoint("Payments", Queue())
    .AddConsumer<PaymentConsumer>()
    // Both keys during the rotation; plaintext on this queue is refused.
    .ProtectPayloads(_ => KeyRing("2026-11", ("2026-10", october), ("2026-11", november)),
        o => o.RequireOnReceive("Payments"))
    .Build();

using var host = builder.Build();
var broker = host.Services.GetRequiredService<INymBroker>();
Console.WriteLine();
Console.WriteLine("Billing starts");
await host.StartAsync();
var inspector = broker.GetQueueInspector("Payments")!;
await WaitUntilAsync(async () => (await inspector.GetCountsAsync()).Active == 0, TimeSpan.FromSeconds(15));
await host.StopAsync();

var failed = await ReadFailedAsync(inspector);
Console.WriteLine();
Console.WriteLine("2. Key rotation");
Check(ledger.Orders.SequenceEqual(["ORD-001", "ORD-002"]), $"billing charged the payments under both keys ({string.Join(", ", ledger.Orders)})");
Check(ledger.Cards.All(c => c == Card), "and saw the decrypted card number");

Console.WriteLine();
Console.WriteLine("3-5. Refused messages are dead-lettered");
Check(failed.TryGetValue("ORD-003", out var legacy) && legacy.Reason == DeadLetterReasons.PayloadNotProtected,
    $"plaintext from the legacy sender: {legacy.Reason}");
Check(failed.TryGetValue("ORD-004", out var rogue) && rogue.Reason == DeadLetterReasons.PayloadUnprotectFailed,
    $"a key billing doesn't have: {rogue.Reason}");
Check(failed.TryGetValue("ORD-005", out var edited) && edited.Reason == DeadLetterReasons.PayloadUnprotectFailed,
    $"a payload edited in the table: {edited.Reason}");
Check(rogue.Payload is not null && !rogue.Payload.Contains(Card) && edited.Payload is not null && !edited.Payload.Contains(Card),
    "the dead-lettered protected messages are still encrypted");

host.Dispose();
SqliteConnection.ClearAllPools();
try { folder.Delete(recursive: true); }
catch (IOException ex) { Console.WriteLine($"Could not delete {folder.FullName}: {ex.Message}"); }

Console.WriteLine();
Console.WriteLine(failures.Count == 0 ? "All checks passed." : $"{failures.Count} check(s) failed.");
return failures.Count == 0 ? 0 : 1;

// A sender: its own broker with a WriteOnly endpoint on the shared table, protected when a protector is given.
async Task PostAsync(IPayloadProtector? protector, PaymentRequested payment)
{
    var services = new ServiceCollection().AddLogging();
    var sender = services.AddNymBroker().AddSqliteEndPoint("Payments", Queue(), EndpointMode.WriteOnly);
    if (protector is not null) sender.ProtectPayloads(protector, o => o.ForEndpoints("Payments"));
    sender.Build();
    await using var provider = services.BuildServiceProvider();
    await provider.GetRequiredService<INymBroker>().PostAsync("Payments", payment);
}

// Someone with write access to the table changes the encrypted payload (they can't read it, so they can't change the
// amount on purpose, but they can still try).
async Task TamperWithLastRowAsync()
{
    await using var connection = new SqliteConnection($"Data Source={db}");
    await connection.OpenAsync();
    await using var read = connection.CreateCommand();
    read.CommandText = "SELECT QueueId, Payload FROM Payments ORDER BY QueueId DESC LIMIT 1";
    await using var reader = await read.ExecuteReaderAsync();
    await reader.ReadAsync();
    var (queueId, envelope) = (reader.GetInt64(0), JsonNode.Parse((byte[])reader[1])!);
    await reader.DisposeAsync();

    var data = Convert.FromBase64String(envelope["message"]!.GetValue<string>());
    data[^20] ^= 0x01;   // flip one bit of the ciphertext
    envelope["message"] = Convert.ToBase64String(data);

    await using var write = connection.CreateCommand();
    write.CommandText = "UPDATE Payments SET Payload = $payload WHERE QueueId = $id";
    write.Parameters.AddWithValue("$payload", Encoding.UTF8.GetBytes(envelope.ToJsonString()));
    write.Parameters.AddWithValue("$id", queueId);
    await write.ExecuteNonQueryAsync();
}

// The rows of one state, as text, read through a WriteOnly broker's inspector.
async Task<List<string>> ReadRowsAsync(QueueInspectionState state)
{
    var services = new ServiceCollection().AddLogging();
    services.AddNymBroker().AddSqliteEndPoint("Payments", Queue(), EndpointMode.WriteOnly).Build();
    await using var provider = services.BuildServiceProvider();
    var page = await provider.GetRequiredService<INymBroker>().GetQueueInspector("Payments")!.GetMessagesAsync(state);
    return page.Messages.Select(m => Encoding.UTF8.GetString(m.Payload.Span)).ToList();
}

// Failed rows by order id. The order id is inside the (encrypted) payload, so the rows are matched by the order they
// were posted in: the inspector lists oldest first.
static async Task<Dictionary<string, (string Reason, string? Payload)>> ReadFailedAsync(IQueueInspector inspector)
{
    var page = await inspector.GetMessagesAsync(QueueInspectionState.Failed);
    var orderIds = new[] { "ORD-003", "ORD-004", "ORD-005" };
    return page.Messages.Select((m, i) => (Id: i < orderIds.Length ? orderIds[i] : m.Id,
            Reason: m.LastError?.Split(':')[0] ?? "", Payload: Encoding.UTF8.GetString(m.Payload.Span)))
        .ToDictionary(r => r.Id, r => (r.Reason, (string?)r.Payload));
}

static string Shorten(string envelope) => envelope.Length <= 160 ? envelope : envelope[..160] + "…";

static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
{
    var until = DateTime.UtcNow + timeout;
    while (!await condition() && DateTime.UtcNow < until)
        await Task.Delay(50);
}

[MessageName("sample.protection.payment-requested")]
public sealed record PaymentRequested(string OrderId, string CardNumber, decimal Amount);

/// <summary>What billing charged.</summary>
public sealed class Ledger
{
    private readonly ConcurrentQueue<PaymentRequested> _charged = new();

    public IEnumerable<string> Orders => _charged.Select(p => p.OrderId).Order();
    public IEnumerable<string> Cards => _charged.Select(p => p.CardNumber);

    public void Charge(PaymentRequested payment) => _charged.Enqueue(payment);
}

public sealed class PaymentConsumer(Ledger ledger) : IConsume<PaymentRequested>
{
    public Task ConsumeAsync(PaymentRequested message, IMessageContext context, CancellationToken ct = default)
    {
        ledger.Charge(message);   // the consumer sees the decrypted message, as without protection
        return Task.CompletedTask;
    }
}
