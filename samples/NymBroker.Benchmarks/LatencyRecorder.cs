using System.Diagnostics.Metrics;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Resilience;

namespace NymBroker.Benchmarks;

public sealed record LatencyStats(double ProcessingP50Ms, double ProcessingP99Ms, double AgeP99Ms, long Failed, long Retries);

/// <summary>
/// Listens to the broker's own metrics during one measured run (<c>--latency</c>). The listener callback runs for
/// every measurement, so it slows the run down — throughput numbers are only comparable between runs without it.
/// </summary>
public sealed class LatencyRecorder : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly List<double> _processingMs = [];
    private readonly List<double> _ageMs = [];
    private long _failed;
    private long _retries;

    public LatencyRecorder()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Name is "nymbroker.message.processing.duration" or "nymbroker.message.age"
                    or "nymbroker.messages.failed"
                && instrument.Meter.Name == NymBrokerDiagnostics.InstrumentationName
                || instrument.Name == "nymbroker.retries" && instrument.Meter.Name == RetryPolicy.InstrumentationName)
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<double>((instrument, value, _, _) =>
        {
            if (instrument.Name == "nymbroker.message.age")
                lock (_ageMs) _ageMs.Add(value * 1000);
            else
                lock (_processingMs) _processingMs.Add(value);
        });
        _listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == "nymbroker.retries") Interlocked.Add(ref _retries, value);
            else Interlocked.Add(ref _failed, value);
        });
        _listener.Start();
    }

    public LatencyStats Stop()
    {
        _listener.Dispose();
        lock (_processingMs) lock (_ageMs)
            return new LatencyStats(
                Percentile(_processingMs, 0.50), Percentile(_processingMs, 0.99), Percentile(_ageMs, 0.99),
                Interlocked.Read(ref _failed), Interlocked.Read(ref _retries));
    }

    public void Dispose() => _listener.Dispose();

    private static double Percentile(List<double> values, double p)
    {
        if (values.Count == 0) return double.NaN;
        values.Sort();
        return values[(int)Math.Ceiling(p * values.Count) - 1];
    }
}
