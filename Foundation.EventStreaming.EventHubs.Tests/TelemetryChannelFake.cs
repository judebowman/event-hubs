using System.Collections.Concurrent;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility.Implementation;

namespace Foundation.EventStreaming.EventHubs.Tests;

public class TelemetryChannelFake : ITelemetryChannel, IAsyncFlushable
{
    public ConcurrentBag<ITelemetry> SentTelemetries = new ConcurrentBag<ITelemetry>();
    public IEnumerable<PageViewTelemetry> SentPageViews => GetTelemetries<PageViewTelemetry>();
    public IEnumerable<EventTelemetry> SentEvents => GetTelemetries<EventTelemetry>();

    public IEnumerable<MetricTelemetry> SentMetrics => GetTelemetries<MetricTelemetry>();

    public IEnumerable<ExceptionTelemetry> SentExceptions => GetTelemetries<ExceptionTelemetry>();

    public IEnumerable<RequestTelemetry> SentRequests => GetTelemetries<RequestTelemetry>();

    public IEnumerable<TraceTelemetry> SentTraces => GetTelemetries<TraceTelemetry>();

    public IEnumerable<DependencyTelemetry> SentDependencies => GetTelemetries<DependencyTelemetry>();

    public IEnumerable<PerformanceCounterTelemetry> SentPerformanceCounters => GetTelemetries<PerformanceCounterTelemetry>();

    public IEnumerable<SessionStateTelemetry> SentSessionStates => GetTelemetries<SessionStateTelemetry>();

    public IEnumerable<OperationTelemetry> SentOperations => GetTelemetries<OperationTelemetry>();
    public bool IsFlushed { get; private set; }
    public bool? DeveloperMode { get; set; }
    public string EndpointAddress { get; set; }
    public void Send(ITelemetry item)
    {
        SentTelemetries.Add(item);
    }
    public void Flush()
    {
        IsFlushed = true;
    }
    public void Dispose()
    {

    }

    public Task<bool> FlushAsync(CancellationToken cancellationToken)
    {
        IsFlushed = true;
        return Task.FromResult(true);
    }

    private IEnumerable<T> GetTelemetries<T>() where T : ITelemetry
    {
        return SentTelemetries
            .Where(t => t is T)
            .Cast<T>();
    }
}