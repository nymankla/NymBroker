using System.Collections.Concurrent;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Idempotency;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Idempotency.Sqlite;

namespace NymBroker.Tests;

/// <summary>
/// <see cref="IInbox"/> (#114): a consumer records its message as handled in its own transaction, so a crash between its
/// commit and the broker's own record no longer has the message processed twice. On a SQLite file, so it always runs.
/// </summary>
public sealed class InboxTests : IAsyncDisposable
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly string FilePath = Path.Combine(Path.GetTempPath(), $"nb_inbox_{Guid.NewGuid():N}.db");
    private static string ConnectionString => $"Data Source={FilePath};Pooling=False";

    [MessageName("tests.inbox.reserve-stock")]
    public sealed class ReserveStock { public Guid Marker { get; set; } = Guid.NewGuid(); public bool MarkHandled { get; set; } public bool Commit { get; set; } = true; }

    /// <summary>Writes a business row, optionally records the message in the same transaction, commits, then "crashes".</summary>
    public sealed class CrashAfterCommitConsumer(IInbox inbox) : IConsume<ReserveStock>
    {
        private static readonly ConcurrentDictionary<Guid, int> Calls = new();
        public static int CallsFor(Guid marker) => Calls.GetValueOrDefault(marker);

        public async Task ConsumeAsync(ReserveStock message, IMessageContext context, CancellationToken ct = default)
        {
            var call = Calls.AddOrUpdate(message.Marker, 1, (_, n) => n + 1);
            await using var connection = new SqliteConnection(ConnectionString);
            await connection.OpenAsync(ct);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(ct);
            await using (var insert = connection.CreateCommand())
            {
                insert.Transaction = transaction;
                insert.CommandText = "CREATE TABLE IF NOT EXISTS reservations (marker TEXT); INSERT INTO reservations VALUES (@marker);";
                insert.Parameters.AddWithValue("@marker", message.Marker.ToString());
                await insert.ExecuteNonQueryAsync(ct);
            }
            if (message.MarkHandled) await inbox.MarkHandledAsync(context, transaction, ct);

            if (message.Commit) await transaction.CommitAsync(ct);
            else await transaction.RollbackAsync(ct);

            // The process dies before the broker records the message: the transport redelivers it.
            if (call == 1) throw new InvalidOperationException("crash after commit");
        }
    }

    private sealed class NativeEndPoint : IEndPointEventDriven
    {
        public EndpointMode Mode => EndpointMode.ReadWrite;
        public bool UsesNativeDeadLetter => true;
        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
        public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct) => Task.CompletedTask;
        public Task StopListeningAsync() => Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        SqliteConnection.ClearAllPools();
        await Task.Yield();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
            if (File.Exists(FilePath + suffix)) File.Delete(FilePath + suffix);
    }

    [Theory]
    [InlineData(true, 1)]    // recorded with the business row: the redelivery is dropped
    [InlineData(false, 2)]   // the window IInbox closes: the redelivery is processed again
    public async Task CrashAfterTheCommit_Redelivery(bool markHandled, int expectedCalls)
    {
        var message = new ReserveStock { MarkHandled = markHandled };

        var results = await DeliverTwiceAsync(message);

        Assert.Equal(ProcessOutcome.Retry, results[0].Outcome);
        Assert.Equal(ProcessOutcome.Completed, results[1].Outcome);
        Assert.Equal(expectedCalls, CrashAfterCommitConsumer.CallsFor(message.Marker));
        Assert.Equal(expectedCalls, await CountReservationsAsync(message.Marker));
    }

    [Fact]
    public async Task RolledBack_IsNotRecorded_SoTheRedeliveryIsProcessed()
    {
        var message = new ReserveStock { MarkHandled = true, Commit = false };

        await DeliverTwiceAsync(message);

        Assert.Equal(2, CrashAfterCommitConsumer.CallsFor(message.Marker));
    }

    [Fact]
    public async Task WithoutATransactionalStore_MarkHandledThrows()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync(Ct);
        await using var transaction = await connection.BeginTransactionAsync(Ct);
        var context = new MessageContext<ReserveStock> { Message = new ReserveStock() };

        await using (var none = Provider(b => b))
            await Assert.ThrowsAsync<InvalidOperationException>(() => none.GetRequiredService<IInbox>().MarkHandledAsync(context, transaction, Ct));
        await using (var inMemory = Provider(b => b.AddIdempotentReceiver()))
            await Assert.ThrowsAsync<InvalidOperationException>(() => inMemory.GetRequiredService<IInbox>().MarkHandledAsync(context, transaction, Ct));

        var noId = new RawMessageContext();   // no id: the broker does not check it, so there is nothing to record
        await using var any = Provider(b => b);
        await any.GetRequiredService<IInbox>().MarkHandledAsync(noId, transaction, Ct);
    }

    private static ServiceProvider Provider(Func<Core.Factory.NymBrokerBuilder, Core.Factory.NymBrokerBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        configure(services.AddNymBroker()).Build();
        return services.BuildServiceProvider();
    }

    private static async Task<ProcessResult[]> DeliverTwiceAsync(ReserveStock message)
    {
        await using var provider = Provider(b => b.AddConsumer<CrashAfterCommitConsumer>()
            .AddSqliteIdempotency(new SqliteIdempotencySettings { ConnectionString = ConnectionString }));
        var broker = provider.GetRequiredService<NymBrokerImpl>();
        broker.AddEndpoint("Native", new NativeEndPoint());
        await broker.StartAsync(Ct);
        try
        {
            var envelope = provider.GetRequiredService<MessageSerializerJson>()
                .SerializeRegistered(new MessageContext<ReserveStock> { Message = message });
            return [await broker.ProcessAsync(envelope, "Native", Ct), await broker.ProcessAsync(envelope, "Native", Ct)];
        }
        finally
        {
            await broker.StopAsync(Ct);
        }
    }

    private static async Task<long> CountReservationsAsync(Guid marker)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        await using DbCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM reservations WHERE marker = '{marker}'";
        return (long)(await command.ExecuteScalarAsync(Ct))!;
    }
}
