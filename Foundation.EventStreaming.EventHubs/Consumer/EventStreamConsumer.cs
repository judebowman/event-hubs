using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs.Processor;
using Foundation.EventStreaming.EventHubs.Exceptions;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.DependencyInjection;

namespace Foundation.EventStreaming.EventHubs.Consumer
{
    public interface IEventStreamConsumer<TEvent> : IDisposable
    {
        Task StartProcessingAsync(CancellationTokenSource cancellationTokenSource);
        Task StopProcessingAsync();
        Func<EventStreamData<TEvent>, IServiceScope, Task> OnEventReceived { get; set; }
        Func<Exception, ProcessEventArgs, IServiceScope, Task> OnFinalException { get; set; }
        Func<ProcessErrorEventArgs, Task> OnEventStreamFailure { get; set; }
    }

    public class EventStreamConsumer<TEvent> : IEventStreamConsumer<TEvent> where TEvent : class
    {
        private bool _hasStarted;
        private volatile bool _allowProcessingEvents = true;
        private CancellationTokenSource _cancellationTokenSource;

        private readonly EventStreamConsumerSettings _settings;
        private readonly IDateTimeProvider _dateTimeProvider;
        private readonly IRetryDelayWithExponentialBackoffProvider _retryDelayProvider;
        private readonly IServiceProvider _serviceProvider;
        private readonly TelemetryClient _telemetryClient;
        private readonly IEventProcessorClient _eventProcessorClient;
        private readonly Timer _timer;

        private readonly ConcurrentDictionary<string, ProcessEventArgs> _lastProcessedEventByPartition;
        private readonly ConcurrentDictionary<string, int> _retryAttemptsByPartition;

        private const int _defaultNumberOfRetries = 3;
        public readonly int NumberOfRetries;
        private const int _defaultSecondsBetweenCheckpointUpdates = 60; 
        private Action<EventStreamConsumerLogItem> _logAction;

        public EventStreamConsumer(
            IEventProcessorClient eventProcessorClient, 
            EventStreamConsumerSettings settings,
            IDateTimeProvider dateTimeProvider,
            IRetryDelayWithExponentialBackoffProvider retryDelayProvider,
            IServiceProvider serviceProvider,
            TelemetryClient telemetryClient)
        {

            ValidateConstructorParameters(eventProcessorClient, settings, dateTimeProvider, retryDelayProvider, serviceProvider, telemetryClient);

            _eventProcessorClient = eventProcessorClient;
            _settings = settings;
            NumberOfRetries = settings.NumberOfRetriesOverride ?? _defaultNumberOfRetries;

            if (settings.LogAction != null)
            {
                AddLogging(settings.LogAction);
            }

            _dateTimeProvider = dateTimeProvider;
            _retryDelayProvider = retryDelayProvider;
            _serviceProvider = serviceProvider;
            _telemetryClient = telemetryClient;

            _lastProcessedEventByPartition = new ConcurrentDictionary<string, ProcessEventArgs>();
            _retryAttemptsByPartition = new ConcurrentDictionary<string, int>();

            var secondsBetweenCheckpointUpdates = settings.SecondsBetweenCheckpointUpdatesOverride ?? _defaultSecondsBetweenCheckpointUpdates;
            _timer = new Timer(SetupCheckpointUpdate, null, 0, secondsBetweenCheckpointUpdates * 1000);
        }

        private static void ValidateConstructorParameters(IEventProcessorClient eventProcessorClient, EventStreamConsumerSettings settings, IDateTimeProvider dateTimeProvider, IRetryDelayWithExponentialBackoffProvider retryDelayProvider, IServiceProvider serviceProvider, TelemetryClient telemetryClient)
        {
            if (eventProcessorClient == null)
            {
                throw new ArgumentNullException(nameof(eventProcessorClient));
            }

            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (dateTimeProvider == null)
            {
                throw new ArgumentNullException(nameof(dateTimeProvider));
            }

            if (retryDelayProvider == null)
            {
                throw new ArgumentNullException(nameof(retryDelayProvider));
            }

            if (serviceProvider == null)
            {
                throw new ArgumentNullException(nameof(serviceProvider));
            }

            if (telemetryClient == null)
            {
                throw new ArgumentNullException(nameof(telemetryClient));
            }
        }

        public Func<EventStreamData<TEvent>, IServiceScope, Task> OnEventReceived { get; set; }
        public Func<Exception, ProcessEventArgs, IServiceScope, Task> OnFinalException { get; set; }
        public Func<ProcessErrorEventArgs, Task> OnEventStreamFailure { get; set; }

        public async Task StartProcessingAsync(CancellationTokenSource cancellationTokenSource)
        {
            if (_hasStarted)
            {
                throw new EventStreamException("The consumer has already been started and cannot be started while running.");
            }

            if (OnEventReceived == null)
            {
                throw new EventStreamSetupException("OnEventReceived must be set before starting the consumer.");
            }

            if (OnFinalException == null)
            {
                throw new EventStreamSetupException("OnFinalException must be set before starting the consumer.");
            }

            if (OnEventStreamFailure == null)
            {
                throw new EventStreamSetupException("OnEventStreamFailure must be set before starting the consumer.");
            }

            _cancellationTokenSource = cancellationTokenSource;

            _eventProcessorClient.ProcessEventAsync += ProcessEventAsync;
            _eventProcessorClient.ProcessErrorAsync += ProcessErrorAsync;

            await _eventProcessorClient.StartProcessingAsync(cancellationTokenSource.Token);
            
            _hasStarted = true;
        }

        private async Task ProcessEventAsync(ProcessEventArgs processEventArgs)
        {
            var receivedAt = _dateTimeProvider.Now;
            Exception receivedException = null;

            using (var operation = _telemetryClient.StartOperation<RequestTelemetry>(_settings.AzureEventHubsName))
            {
                var serviceScopeFactory = TryGetServiceScopeFactory();

                using (var scope = serviceScopeFactory?.CreateScope())
                {
                    try
                    {
                        if (ShouldReturnImmediately(processEventArgs))
                        {
                            return;
                        }

                        SetTelemetryOperationProperties(processEventArgs, operation);

                        EventStreamData<TEvent> eventStreamData;
                        try
                        {
                            eventStreamData = MapToEventStreamData(processEventArgs);
                        }
                        catch (Exception exception)
                        {
                            // If we can't deserialize the event, we want to handle it as a poison event
                            try
                            {
                                await HandlePoisonEventException(exception, processEventArgs, scope);
                                return;
                            }
                            catch (Exception ex)
                            {
                                receivedException = ex;
                                operation.Telemetry.Success = false;

                                await HandleCriticalException(processEventArgs, ex);
                                return;
                            }
                        }

                        await OnEventReceived(eventStreamData, scope);

                        SetLastProcessedEventForPartition(processEventArgs);

                        ResetRetryAttemptsForPartition(processEventArgs.Partition.PartitionId);

                        operation.Telemetry.Success = true;
                    }
                    catch (Exception exception)
                    {
                        try
                        {
                            receivedException = exception;
                            operation.Telemetry.Success = false;
                            
                            await HandleException(exception, processEventArgs, scope);
                        }
                        catch (Exception ex)
                        {
                            receivedException = ex;
                            operation.Telemetry.Success = false;

                            await HandleCriticalException(processEventArgs, ex);
                        }
                    }
                    finally
                    {
                        LogEvent(processEventArgs, receivedAt, receivedException);
                        _telemetryClient.StopOperation(operation);
                    }
                }
            }
        }

        private bool ShouldReturnImmediately(ProcessEventArgs processEventArgs)
        {
            return processEventArgs.CancellationToken.IsCancellationRequested
                   || !_allowProcessingEvents
                   || !processEventArgs.HasEvent;
        }

        private void SetTelemetryOperationProperties(ProcessEventArgs processEventArgs, IOperationHolder<RequestTelemetry> operation)
        {
            operation.Telemetry.Properties.Add(TelemetryPropertyNames.EventHubName, _settings.AzureEventHubsName);
            operation.Telemetry.Properties.Add(TelemetryPropertyNames.ConsumerGroupName, _settings.ConsumerGroupName);
            operation.Telemetry.Properties.Add(TelemetryPropertyNames.PartitionId, processEventArgs.Partition.PartitionId);
            operation.Telemetry.Properties.Add(TelemetryPropertyNames.Offset, processEventArgs.Data.Offset.ToString());
            operation.Telemetry.Properties.Add(TelemetryPropertyNames.SequenceNumber, processEventArgs.Data.SequenceNumber.ToString());
            operation.Telemetry.Properties.Add(TelemetryPropertyNames.EnqueuedTime, processEventArgs.Data.EnqueuedTime.ToString());
            operation.Telemetry.Properties.Add(TelemetryPropertyNames.MessageId, processEventArgs.Data.MessageId);
        }

        private void LogEvent(ProcessEventArgs processEventArgs, DateTime receivedAt, Exception receivedException)
        {
            try
            {
                if (processEventArgs.HasEvent == false || _logAction == null)
                {
                    return;
                }

                _logAction(new EventStreamConsumerLogItem
                {
                    EventId = processEventArgs.Data.MessageId,
                    PartitionKey = processEventArgs.Data.PartitionKey,
                    PartitionId = processEventArgs.Partition.PartitionId,
                    EventHubName = _settings.AzureEventHubsName,
                    ConsumerGroupName = _settings.ConsumerGroupName,
                    Exception = receivedException,
                    EventContent = processEventArgs.Data.EventBody.ToString(),
                    Offset = processEventArgs.Data.Offset,
                    SequenceNumber = processEventArgs.Data.SequenceNumber,
                    Timestamp = receivedAt,
                    EnqueuedTime = processEventArgs.Data.EnqueuedTime,
                });
            }
            catch (Exception exception)
            {
                TrackException(new TelemetryTrackingExceptionInfo { ProcessEventArgs = processEventArgs, Exception = exception});
                // continue after logging -- do not let a failure in logging break this process
            }
        }

        private async Task HandleException(Exception exception, ProcessEventArgs processEventArgs, IServiceScope scope)
        {
            var retryCount = GetRetryCountForPartition(processEventArgs.Partition.PartitionId);

            if (retryCount < NumberOfRetries)
            {
                TrackException(new TelemetryTrackingExceptionInfo { ProcessEventArgs = processEventArgs, Exception = exception });

                retryCount++;

                SetRetryCountForPartition(processEventArgs.Partition.PartitionId, retryCount);

                await WaitForRetryDelay(retryCount);

                await ProcessEventAsync(processEventArgs);
            }
            else
            {
                TrackException(new TelemetryTrackingExceptionInfo
                {
                    ProcessEventArgs = processEventArgs, 
                    Exception = exception,
                    IsFinalException = true
                });

                ResetRetryAttemptsForPartition(processEventArgs.Partition.PartitionId);

                await OnFinalException(exception, processEventArgs, scope);
                
                // Even though the event failed with potential retries, we still want to mark this event as having been processed so that we don't get stuck in an infinite loop.
                // It will be up to the implementation to handle the failure and potentially save the event to a dead letter store.
                SetLastProcessedEventForPartition(processEventArgs);
            }
        }

        private async Task HandlePoisonEventException(Exception exception, ProcessEventArgs processEventArgs, IServiceScope scope)
        {
            TrackException(new TelemetryTrackingExceptionInfo
            {
                ProcessEventArgs = processEventArgs,
                Exception = exception,
                IsPoisonEvent = true,
                IsDeserializationException = true
            });
            
            await OnFinalException(exception, processEventArgs, scope);
            
            // Even though the event failed, we still want to mark this event as having been processed so that we don't get stuck in an infinite loop.
            // It will be up to the implementation to handle the failure and potentially save the event to a dead letter store.
            SetLastProcessedEventForPartition(processEventArgs);
        }

        private async Task HandleCriticalException(ProcessEventArgs processEventArgs, Exception exception)
        {
            _allowProcessingEvents = false;

            try
            {
                TrackException(new TelemetryTrackingExceptionInfo
                {
                    ProcessEventArgs = processEventArgs,
                    Exception = exception,
                    IsCriticalException = true
                });

                await SaveCheckpointsIfApplicable();
            }
            finally
            {
                // This should trigger the cancellation token to stop processing, which will stop the hosted service and the application hosting it.
                _cancellationTokenSource.Cancel();
            }
        }

        private Task WaitForRetryDelay(int retryCount)
        {
            return _retryDelayProvider.WaitForRetryDelay(retryCount);
        }

        private async Task ProcessErrorAsync(ProcessErrorEventArgs processErrorEventArgs)
        {
            TrackException(new TelemetryTrackingExceptionInfo { ProcessErrorEventArgs = processErrorEventArgs });
            try
            {
                await OnEventStreamFailure(processErrorEventArgs);
            }
            catch (Exception exception)
            {
                TrackException(new TelemetryTrackingExceptionInfo() { Exception = exception});
            }
        }

        private void TrackException(TelemetryTrackingExceptionInfo trackingInfo)
        {
            var telemetryProperties = new Dictionary<string, string>
            {
                { TelemetryPropertyNames.EventHubName, _settings.AzureEventHubsName },
                { TelemetryPropertyNames.ConsumerGroupName, _settings.ConsumerGroupName }
            };

            if (trackingInfo.ProcessEventArgs != null)
            {
                telemetryProperties.Add(TelemetryPropertyNames.PartitionId, trackingInfo.ProcessEventArgs.Value.Partition?.PartitionId);
                telemetryProperties.Add(TelemetryPropertyNames.Offset, trackingInfo.ProcessEventArgs.Value.Data?.Offset.ToString());
                telemetryProperties.Add(TelemetryPropertyNames.SequenceNumber, trackingInfo.ProcessEventArgs.Value.Data?.SequenceNumber.ToString());
                telemetryProperties.Add(TelemetryPropertyNames.EnqueuedTime, trackingInfo.ProcessEventArgs.Value.Data?.EnqueuedTime.ToString());
                telemetryProperties.Add(TelemetryPropertyNames.MessageId, trackingInfo.ProcessEventArgs.Value.Data?.MessageId);
            }

            if (trackingInfo.ProcessErrorEventArgs != null)
            {
                telemetryProperties.Add(TelemetryPropertyNames.Operation, trackingInfo.ProcessErrorEventArgs.Value.Operation);
                telemetryProperties.Add(TelemetryPropertyNames.PartitionId, trackingInfo.ProcessErrorEventArgs.Value.PartitionId);
                telemetryProperties.Add(TelemetryPropertyNames.IsEventProcessorClientError, "true");
                trackingInfo.Exception = trackingInfo.ProcessErrorEventArgs.Value.Exception;
            }

            if (trackingInfo.IsPoisonEvent.HasValue)
            {
                telemetryProperties.Add(TelemetryPropertyNames.IsPoisonEvent, trackingInfo.IsPoisonEvent.Value.ToString());
            }

            if (trackingInfo.IsDeserializationException.HasValue)
            {
                telemetryProperties.Add(TelemetryPropertyNames.IsDeserializationException, trackingInfo.IsDeserializationException.Value.ToString());
            }

            if (trackingInfo.IsCriticalException.HasValue)
            {
                telemetryProperties.Add(TelemetryPropertyNames.IsCriticalException, trackingInfo.IsCriticalException.Value.ToString());
            }

            _telemetryClient.TrackException(trackingInfo.Exception, telemetryProperties);
        }

        public async Task StopProcessingAsync()
        {
            try
            {
                _allowProcessingEvents = false;
                await SaveCheckpointsIfApplicable();
                await _eventProcessorClient.StopProcessingAsync();
            }
            catch (Exception exception)
            {
                TrackException(new TelemetryTrackingExceptionInfo { Exception = exception });
                throw;
            }
            finally
            {
                await _telemetryClient.FlushAsync(default);
            }
        }

        public void AddLogging(Action<EventStreamConsumerLogItem> logAction)
        {
            _logAction += logAction;
        }

        private void SetLastProcessedEventForPartition(ProcessEventArgs processEventArgs)
        {
            _lastProcessedEventByPartition[processEventArgs.Partition.PartitionId] = processEventArgs;
        }

        private IServiceScopeFactory TryGetServiceScopeFactory()
        {
            try
            {
                return _serviceProvider.GetService<IServiceScopeFactory>();
            }
            catch (Exception)
            {
                return null;
            }
        }

        private void SetRetryCountForPartition(string partitionId, int retryCount)
        {
            _retryAttemptsByPartition[partitionId] = retryCount;
        }

        private int GetRetryCountForPartition(string partitionId)
        {
            _retryAttemptsByPartition.TryGetValue(partitionId, out var retryCount);
            return retryCount;
        }

        private void ResetRetryAttemptsForPartition(string partitionId)
        {
            _retryAttemptsByPartition[partitionId] = 0;
        }

        private EventStreamData<TEvent> MapToEventStreamData(ProcessEventArgs processEventArgs)
        {
            var deserializedEvent = GetDeserializedEvent(processEventArgs);
            if (Guid.TryParse(processEventArgs.Data.MessageId, out var eventId) == false)
            {
                throw new EventStreamException("The event message id is not a valid GUID.");
            }
            var properties = processEventArgs.Data.Properties;

            return new EventStreamData<TEvent>(eventId, deserializedEvent, properties);
        }

        private TEvent GetDeserializedEvent(ProcessEventArgs processEventArgs)
        {
            if (typeof(TEvent) == typeof(string))
            {
                return processEventArgs.Data.EventBody.ToString() as TEvent;
            }

            if (typeof(TEvent) == typeof(byte[]))
            {
                return processEventArgs.Data.EventBody.ToArray() as TEvent;
            }

            var result = JsonSerializer.Deserialize<TEvent>(processEventArgs.Data.EventBody.ToString());
            return result;
        }

        private void SetupCheckpointUpdate(object state)
        {
            // Discard the result
            // If we were on .NET 6 or above, we could utilize PeriodicTimer, which allows you to await async methods, whereas Timer does not.
            // However, we have to support older versions of .NET Core and Framework.
            _ = SaveCheckpointsIfApplicable();
        }

        private async Task SaveCheckpointsIfApplicable()
        {
            foreach (var keyValuePair in _lastProcessedEventByPartition)
            {
                var lastProcessedEvent = keyValuePair.Value;
                await lastProcessedEvent.UpdateCheckpointAsync();
                _lastProcessedEventByPartition.TryRemove(keyValuePair.Key, out _);
            }
        }

        public void Dispose()
        {
            _cancellationTokenSource?.Dispose();
            _timer?.Dispose();
        }
    }
}
