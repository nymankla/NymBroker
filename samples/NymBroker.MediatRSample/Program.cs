using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.File;
using NymBroker.Core.Impl;
using NymBroker.Endpoint.Sqlite;
using NymBroker.MediatRSample.Orders;

// Usage:
//   dotnet run --project samples/NymBroker.MediatRSample                  # API on http://localhost:5000, commands in memory
//   dotnet run --project samples/NymBroker.MediatRSample -- --sqlite      # commands in a SQLite queue (survive restarts, retried)
//   dotnet run --project samples/NymBroker.MediatRSample -- --demo        # runs the CQRS flow against itself, then exits

var useSqlite = args.Contains("--sqlite");
var demo = args.Contains("--demo");

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<OrderStore>();
builder.Services.AddSingleton<OrderQueries>();
builder.Services.AddSingleton<AuditFilter>();

var broker = builder.Services.AddNymBroker();

// The one line that differs between "in-process like MediatR" and "durable like a message queue".
if (useSqlite)
    broker.AddSqliteEndPoint("Commands", new SqliteSettings { ConnectionString = "Data Source=mediatr-sample.db", TableName = "Commands" });
else
    broker.AddMemoryEndPoint("Commands");

broker
    .AddConsumer<CreateOrderHandler>()                                     // IRequestHandler: one handler per command
    .AddTopic<OrderCreated>("orders.events")                               // INotification: any number of handlers
        .SubscribeWith<OrderSummaryProjection>()
        .SubscribeWith<SendConfirmationEmail>()
        .Build()
    .AddIdempotentReceiver()                                               // drops redelivered duplicates by message id
    .AddFileEndPoint("DeadLetters", new FileSettings { PostPath = "dead-letters" }, EndpointMode.WriteOnly)
    .WithDeadLetterEndpoint("DeadLetters")                                 // failed messages are kept, not lost
    .Build();

if (demo)
    builder.WebHost.UseUrls("http://127.0.0.1:0");   // any free port

var app = builder.Build();
app.Services.GetRequiredService<INymBroker>().AddFilter(app.Services.GetRequiredService<AuditFilter>());

// Command side: validate, enqueue, answer 202 with where to look. The handler runs later.
app.MapPost("/orders", async (PlaceOrderRequest request, INymBroker nymBroker, CancellationToken ct) =>
{
    var errors = new Dictionary<string, string[]>();
    if (string.IsNullOrWhiteSpace(request.Customer)) errors["customer"] = ["Customer is required."];
    if (request.Amount <= 0) errors["amount"] = ["Amount must be greater than zero."];
    if (errors.Count > 0) return Results.ValidationProblem(errors);

    var orderId = Guid.NewGuid();
    await nymBroker.PostAsync("Commands", new CreateOrder(orderId, request.Customer, request.Amount), ct);
    return Results.Accepted($"/orders/{orderId}", new { orderId });
});

// Query side: read the read model directly; no broker involved.
app.MapGet("/orders/{id:guid}", (Guid id, OrderQueries queries) =>
    queries.Get(id) is { } summary
        ? Results.Ok(summary)
        : Results.NotFound(new { message = "Unknown order, or not projected yet (the read model is eventually consistent)." }));

app.MapGet("/orders", (OrderQueries queries) => queries.All());

if (!demo)
{
    app.Run();
    return 0;
}

// --- Demo: the whole CQRS round trip over HTTP, with checks; exit code 1 if anything is off. ---
await app.StartAsync();
var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
using var http = new HttpClient { BaseAddress = new Uri(address) };
var failures = 0;
void Check(bool ok, string what) { Console.WriteLine($"  [{(ok ? "ok" : "FAIL")}] {what}"); if (!ok) failures++; }

Console.WriteLine($"Demo against {address} ({(useSqlite ? "SQLite" : "Memory")} command queue)");

var invalid = await http.PostAsJsonAsync("/orders", new PlaceOrderRequest("", 0));
Check(invalid.StatusCode == HttpStatusCode.BadRequest, "invalid command is rejected with 400 before it is queued");

var accepted = await http.PostAsJsonAsync("/orders", new PlaceOrderRequest("Alice", 499.90m));
Check(accepted.StatusCode == HttpStatusCode.Accepted, "valid command is accepted with 202");
var orderId = (await accepted.Content.ReadFromJsonAsync<AcceptedOrder>())!.OrderId;

var stopwatch = Stopwatch.StartNew();
OrderSummary? summary = null;
while (summary is null && stopwatch.Elapsed < TimeSpan.FromSeconds(5))
{
    var response = await http.GetAsync($"/orders/{orderId}");
    if (response.IsSuccessStatusCode) summary = await response.Content.ReadFromJsonAsync<OrderSummary>();
    else await Task.Delay(10);
}
Check(summary is { Customer: "Alice", Amount: 499.90m }, $"query sees the order after {stopwatch.ElapsedMilliseconds} ms (eventual consistency)");

await app.StopAsync();
Console.WriteLine(failures == 0 ? "Demo passed." : $"Demo failed: {failures} check(s).");
return failures == 0 ? 0 : 1;

public sealed record PlaceOrderRequest(string Customer, decimal Amount);
public sealed record AcceptedOrder(Guid OrderId);
