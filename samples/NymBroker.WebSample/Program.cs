using NymBroker.Core.DI;
using NymBroker.Core;
using NymBroker.Core.Endpoint;
using NymBroker.WebSample;
using NymBroker.Dashboard;
using NymBroker.Endpoint.Sqlite;
using NymBroker.WebSample.Consumers;
using NymBroker.WebSample.Messages;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.AddKeyedSingleton<IEndPoint>("PriorityAudit", (sp, _) => new AuditLogEndPoint(sp.GetRequiredService<ILogger<AuditLogEndPoint>>()));

var nymBroker = builder.Services.AddNymBroker()
    .AddSqliteEndPoint("SqlQueue", new SqliteSettings
    {
        ConnectionString = "Data Source=websample.db",
        AutoCreateTable  = true,
        LeaseTimeout     = TimeSpan.FromMinutes(5),
        MaxRetryCount    = 3
    })
    // A copy of every high-priority order goes to an audit log, so the dashboard's flow map has a route to show.
    .AddRoute<OrderMessage>(r => r.To("PriorityAudit").WhenFrom("SqlQueue").When(o => o.GetProperty("priority").GetString() == "high"))
    .AddConsumer<OrderConsumer>()
    .AddScheduledAction("nightly-summary", "0 2 * * *", (_, _) => Task.CompletedTask);
nymBroker.RegisterEndpoint("PriorityAudit");   // the AuditLogEndPoint registered above
nymBroker.Build();

// The dashboard: endpoints, queue contents, health and live throughput at /nymbroker. Without an authorization
// policy it answers only requests from this machine; add .RequireAuthorization(...) before opening it to others.
builder.Services.AddNymBrokerDashboard(o => o.Title = "Web sample");

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();
app.MapNymBrokerDashboard();
app.MapGet("/", () => Results.Redirect("/nymbroker/")).ExcludeFromDescription();

app.MapPost("/orders", async (OrderMessage order, INymBroker broker) =>
{
    await broker.PostAsync("SqlQueue", order);
    return Results.Accepted();
});

// Posts a burst of orders so the dashboard has something to show; one in ten has a negative amount, which the
// consumer rejects, so it is retried and then ends up as Failed in the queue view.
app.MapPost("/orders/demo", async (INymBroker broker, int? count) =>
{
    var orders = Enumerable.Range(1, Math.Clamp(count ?? 200, 1, 10_000)).Select(i => new OrderMessage(
        OrderId: $"DEMO-{Guid.NewGuid():N}"[..13],
        Customer: $"Customer {i % 17}",
        Amount: i % 10 == 0 ? -1m : Random.Shared.Next(10, 500),
        Priority: i % 4 == 0 ? "high" : "normal"));
    await broker.PostBatchAsync("SqlQueue", orders);
    return Results.Accepted();
});

app.Run();
