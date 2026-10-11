using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.EfCoreTransactionSample;
using NymBroker.Endpoint.SqlServer;
using NymBroker.Idempotency.SqlServer;

// Start SQL Server first: ./scripts/setup-sqlserver.ps1
// Runs three scenarios against a throw-away database "nymbroker_ef_sample", checks each one, and exits 0 or 1.
// The default connection string is the local container's (TrustServerCertificate=True is for local development only;
// see docs/security.md). Override it with NYMBROKER_SQLSERVER_CS.

const string QueueTable = "dbo.ef_sample_queue";
const string IdempotencyTable = "dbo.ef_sample_idempotency";
const int MaxRetryCount = 3;

var connectionString = new SqlConnectionStringBuilder(
    Environment.GetEnvironmentVariable("NYMBROKER_SQLSERVER_CS")
    ?? "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True")
{
    InitialCatalog = "nymbroker_ef_sample"
}.ConnectionString;

var builder = Host.CreateApplicationBuilder(args);
// EF Core logging: the SQL it runs (Information) and the transaction steps — begin / commit / rollback (Debug) — so the
// output shows each message's transaction. Everything else from EF Core only from Warning up.
builder.Logging.SetMinimumLevel(LogLevel.Debug);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Command", LogLevel.Information);
builder.Logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Transaction", LogLevel.Debug);
builder.Logging.AddFilter("NymBroker", LogLevel.Information);
builder.Logging.AddFilter("NymBroker.Core.Impl.NymBrokerImpl", LogLevel.Debug);   // shows "Dropping duplicate message …"

// Scoped (the default for AddDbContext): one DbContext per message, because the broker creates a DI scope per dispatch.
builder.Services.AddDbContext<OrdersDbContext>(o => o
    .UseSqlServer(connectionString)
    // Shows parameter values in the logged SQL. Sample only: in production it writes message data into the logs.
    .EnableSensitiveDataLogging());

builder.Services.AddNymBroker()
    .AddSqlServerEndPoint("Orders", new SqlServerSettings
    {
        ConnectionString = connectionString,
        TableName        = QueueTable,
        MaxRetryCount    = MaxRetryCount,
        PollInterval     = TimeSpan.FromMilliseconds(50)
    })
    // Idempotent receiver: the broker claims each message id in this table before the consumer runs, remembers it once
    // the message is handled, and drops a later delivery of the same id. A Retry releases the claim.
    .AddSqlServerIdempotency(new SqlServerIdempotencySettings { ConnectionString = connectionString, TableName = IdempotencyTable })
    .AddConsumer<PlaceOrderConsumer>()
    .Build();

var host = builder.Build();

// Fresh sample database (EnsureCreated only creates tables in a database that has none — hence a dedicated database),
// with two products in stock.
await using (var scope = host.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    await db.Database.EnsureDeletedAsync();
    await db.Database.EnsureCreatedAsync();
    db.Products.AddRange(
        new Product { ProductId = "WIDGET", Name = "Widget", Stock = 10 },
        new Product { ProductId = "GADGET", Name = "Gadget", Stock = 1 });
    await db.SaveChangesAsync();
}

await host.StartAsync();
var broker = host.Services.GetRequiredService<INymBroker>();
var failures = 0;
void Check(bool ok, string what) { Console.WriteLine($"  [{(ok ? "ok" : "FAIL")}] {what}"); if (!ok) failures++; }

// --- 1. Normal message: order, lines and stock change in one transaction -------------------------------------------
Console.WriteLine("1. Place an order: order + lines + stock in one transaction");
await broker.PostAsync("Orders", new PlaceOrder("ORD-1", "Alice", [new("WIDGET", 2)]));
await WaitUntilAsync(async db => await db.Orders.AnyAsync(o => o.OrderId == "ORD-1"));
Check(await QueryAsync(db => db.OrderLines.CountAsync(l => l.OrderId == "ORD-1")) == 1
      && await StockAsync("WIDGET") == 8, "order, its line and WIDGET stock 10 → 8 committed together");
await WaitUntilAsync(async _ => await CompletedIdempotencyRowsAsync() == 1);

// --- 2. Redelivery: the same envelope (same message id) arrives twice -----------------------------------------------
Console.WriteLine("2. Deliver the same message twice (what a redelivery after a lost ack looks like)");
var envelope = Envelope(Guid.NewGuid(), new PlaceOrder("ORD-2", "Bob", [new("WIDGET", 1)]));
await broker.PostAsync("Orders", (Stream)new MemoryStream(envelope));   // typed as Stream: posts the bytes as-is
await broker.PostAsync("Orders", (Stream)new MemoryStream(envelope));
await WaitUntilAsync(async _ => await QueueCountAsync(status: 2) == 3);   // 3 rows Completed: ORD-1 and ORD-2 twice
Check(await QueryAsync(db => db.Orders.CountAsync(o => o.OrderId == "ORD-2")) == 1
      && await StockAsync("WIDGET") == 7, "applied once (WIDGET 8 → 7, not 6); the broker drops the second delivery as a duplicate");

// --- 3. Failure part-way: the 2nd line is out of stock after the 1st line's stock was already reserved --------------
Console.WriteLine("3. Out of stock on the second line: everything rolls back");
await broker.PostAsync("Orders", new PlaceOrder("ORD-3", "Carol", [new("WIDGET", 3), new("GADGET", 5)]));
await WaitUntilAsync(async _ => await QueueCountAsync(status: 3) == 1);
Check(!await QueryAsync(db => db.Orders.AnyAsync(o => o.OrderId == "ORD-3"))
      && !await QueryAsync(db => db.OrderLines.AnyAsync(l => l.OrderId == "ORD-3")), "no order and no lines for ORD-3");
Check(await StockAsync("WIDGET") == 7 && await StockAsync("GADGET") == 1, "WIDGET's reservation (7 → 4) was rolled back too: WIDGET 7, GADGET 1");
Check(await CompletedIdempotencyRowsAsync() == 2, "the failed message is not remembered (its claim was released), so a later replay is processed");
var (attempts, lastError) = await FailedRowAsync();
Check(attempts == MaxRetryCount && lastError.Contains("not enough stock"), $"the queue retried it {attempts} times, then marked it Failed: \"{lastError}\"");

await host.StopAsync();
Console.WriteLine(failures == 0 ? "Demo passed." : $"Demo failed: {failures} check(s).");
return failures == 0 ? 0 : 1;

// --- helpers --------------------------------------------------------------------------------------------------------

async Task<T> QueryAsync<T>(Func<OrdersDbContext, Task<T>> query)
{
    await using var scope = host.Services.CreateAsyncScope();
    return await query(scope.ServiceProvider.GetRequiredService<OrdersDbContext>());
}

async Task WaitUntilAsync(Func<OrdersDbContext, Task<bool>> condition)
{
    var deadline = DateTime.UtcNow.AddSeconds(15);
    while (!await QueryAsync(condition))
    {
        if (DateTime.UtcNow > deadline) throw new TimeoutException("The expected state was not reached in 15 s.");
        await Task.Delay(50);
    }
}

async Task<int> QueueCountAsync(int status)
{
    await using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();
    await using var cmd = new SqlCommand($"SELECT COUNT(*) FROM {QueueTable} WHERE status = @status", conn);
    cmd.Parameters.AddWithValue("@status", status);
    return (int)(await cmd.ExecuteScalarAsync())!;
}

Task<int> StockAsync(string productId)
    => QueryAsync(db => db.Products.Where(p => p.ProductId == productId).Select(p => p.Stock).SingleAsync());

async Task<int> CompletedIdempotencyRowsAsync()
{
    await using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();
    await using var cmd = new SqlCommand($"SELECT COUNT(*) FROM {IdempotencyTable} WHERE status = 2", conn);   // 2 = completed
    return (int)(await cmd.ExecuteScalarAsync())!;
}

async Task<(int Attempts, string LastError)> FailedRowAsync()
{
    await using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();
    await using var cmd = new SqlCommand($"SELECT attempt_count, last_error FROM {QueueTable} WHERE status = 3", conn);
    await using var reader = await cmd.ExecuteReaderAsync();
    await reader.ReadAsync();
    return (reader.GetInt32(0), reader.IsDBNull(1) ? "" : reader.GetString(1));
}

// A NymBroker envelope with a fixed id, so two posts look like one message delivered twice.
static byte[] Envelope(Guid id, PlaceOrder order) => Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
{
    id,
    correlationId = Guid.NewGuid(),
    messageType = "orders.place-order",
    created = DateTime.UtcNow,
    message = order
}, JsonSerializerOptions.Web));
