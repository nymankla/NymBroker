using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Sql;
using NymBroker.WebSample.Consumers;
using NymBroker.WebSample.Messages;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.AddNymBroker()
    .AddSqliteEndPoint("SqlQueue", new SqliteSettings
    {
        ConnectionString = "Data Source=websample.db",
        AutoCreateTable  = true,
        LeaseTimeout     = TimeSpan.FromMinutes(5),
        MaxRetryCount    = 5
    })
    .AddConsumer<OrderConsumer>()
    .Build();

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();
app.MapGet("/", () => Results.Redirect("/scalar/v1")).ExcludeFromDescription();

app.MapPost("/orders", async (OrderMessage order, INymBroker broker) =>
{
    await broker.PostAsync("SqlQueue", order);
    return Results.Accepted();
});

app.Run();
