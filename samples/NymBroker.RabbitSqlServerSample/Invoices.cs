using System.Collections.Concurrent;
using NymBroker.Core.Consume;
using NymBroker.Core.Message;

namespace NymBroker.RabbitSqlServerSample;

[MessageName("sample.invoice")]
public sealed record Invoice(string Number, bool FailFirstAttempt = false);

/// <summary>The name of this worker, for the output.</summary>
public sealed record WorkerName(string Value);

/// <summary>Counts what happened. <c>--demo</c> shares one between its workers to check the result.</summary>
public sealed class Recorder
{
    private readonly ConcurrentDictionary<string, int> _attempts = new();
    private readonly ConcurrentDictionary<string, int> _handled = new();
    private readonly ConcurrentQueue<DateTime> _scheduledRuns = new();

    public int NextAttempt(string number) => _attempts.AddOrUpdate(number, 1, static (_, n) => n + 1);
    public void Handled(string number) => _handled.AddOrUpdate(number, 1, static (_, n) => n + 1);
    public void ScheduledRun(DateTime slot) => _scheduledRuns.Enqueue(slot);

    public int Attempts(string number) => _attempts.GetValueOrDefault(number);
    public int TimesHandled(string number) => _handled.GetValueOrDefault(number);
    public IReadOnlyCollection<DateTime> ScheduledRuns => _scheduledRuns;
}

public sealed class InvoiceConsumer(Recorder recorder, WorkerName worker) : IConsume<Invoice>
{
    public Task ConsumeAsync(Invoice message, IMessageContext context, CancellationToken ct = default)
    {
        if (recorder.NextAttempt(message.Number) == 1 && message.FailFirstAttempt)
        {
            Console.WriteLine($"  [{worker.Value}] {message.Number}: failed, RabbitMQ redelivers it");
            throw new InvalidOperationException($"Could not send {message.Number}.");
        }

        recorder.Handled(message.Number);
        Console.WriteLine($"  [{worker.Value}] {message.Number}: sent");
        return Task.CompletedTask;
    }
}
