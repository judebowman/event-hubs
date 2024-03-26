using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Foundation.EventStreaming.EventHubs.Consumer;
using Foundation.EventStreaming.EventHubs.Exceptions;
using Foundation.EventStreaming.EventHubs.Producer;
using Microsoft.ApplicationInsights;

namespace Foundation.EventStreaming.EventHubs
{
    public interface IEventStreamFactory : IAsyncDisposable
    {
        IEventStreamProducer CreateProducer(EventStreamProducerSettings settings);

        IEventStreamConsumer<TEvent> CreateConsumer<TEvent>(EventStreamConsumerSettings settings) where TEvent : class;
    }

    public class EventStreamFactory : IEventStreamFactory
    {
        private readonly IDateTimeProvider _dateTimeProvider;
        private readonly IRetryDelayWithExponentialBackoffProvider _retryDelayWithExponentialBackoffProvider;
        private readonly TelemetryClient _telemetryClient;
        private readonly IServiceProvider _serviceProvider;
        private readonly ConcurrentDictionary<string, EventStreamProducer> _producersCache = new ConcurrentDictionary<string, EventStreamProducer>();

        private static Action<EventStreamConsumerLogItem> _consumerLogAction;
        private static Action<EventStreamProducerLogItem> _producerLogAction;
        private static bool _producersHaveBeenCreated;
        private static bool _consumersHaveBeenCreated;

        public EventStreamFactory(
            IDateTimeProvider dateTimeProvider, 
            IRetryDelayWithExponentialBackoffProvider retryDelayWithExponentialBackoffProvider,
            TelemetryClient telemetryClient, 
            IServiceProvider serviceProvider)
        {
            _dateTimeProvider = dateTimeProvider;
            _retryDelayWithExponentialBackoffProvider = retryDelayWithExponentialBackoffProvider;
            _telemetryClient = telemetryClient;
            _serviceProvider = serviceProvider;
        }

        public static void AddGlobalConsumerLogging(Action<EventStreamConsumerLogItem> logAction)
        {
            if (_consumersHaveBeenCreated)
            {
                const string message = "One or more consumers have already been created.  Calls to this method should be made during application start up before any consumers have been created.";
                throw new EventStreamException(message);
            }
            _consumerLogAction = logAction;
        }

        public static void AddGlobalProducerLogging(Action<EventStreamProducerLogItem> logAction)
        {
            if (_producersHaveBeenCreated)
            {
                const string message = "One or more producers have already been created.  Calls to this method should be made during application start up before any producers have been created.";
                throw new EventStreamException(message);
            }
            _producerLogAction = logAction;
        }

        public IEventStreamProducer CreateProducer(EventStreamProducerSettings settings)
        {
            settings.Validate();

            var dictionaryKey = $"{settings.AzureEventHubsFullyQualifiedNamespace}:{settings.AzureEventHubsName}";
            if (_producersCache.TryGetValue(dictionaryKey, out var producer))
            {
                return producer;
            }

            var eventHubProducerClient = GetEventHubProducerClient(settings);
            producer = new EventStreamProducer(eventHubProducerClient, _dateTimeProvider);
            _producersCache.TryAdd(dictionaryKey, producer);
            if (_producerLogAction != null)
            {
                producer.AddLogging(_producerLogAction);
            }
            _producersHaveBeenCreated = true;
            return producer;
        }

        public IEventStreamConsumer<TEvent> CreateConsumer<TEvent>(
            EventStreamConsumerSettings settings) where TEvent : class
        {
            settings.Validate();

            var eventProcessorClient = GetEventProcessorClient(settings);

            var consumer = new EventStreamConsumer<TEvent>(
                eventProcessorClient, 
                settings, 
                _dateTimeProvider, 
                _retryDelayWithExponentialBackoffProvider,
                _serviceProvider, 
                _telemetryClient);

            if (_consumerLogAction != null)
            {
                consumer.AddLogging(_consumerLogAction);
            }
            _consumersHaveBeenCreated = true;
            return consumer;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var eventStreamProducer in _producersCache.Values)
            {
                await eventStreamProducer.CloseAsync();
            }
        }

        private static IEventHubProducerClient GetEventHubProducerClient(EventStreamProducerSettings settings)
        {
            settings.Validate();

            return new EventHubProducerClientWrapper(
                settings.AzureEventHubsFullyQualifiedNamespace,
                settings.AzureEventHubsName,
                settings.AzureTokenCredential);
        }

        private IEventProcessorClient GetEventProcessorClient(EventStreamConsumerSettings settings)
        {
            return new EventProcessorClientWrapper(
                GetBlobContainerClient(settings),
                settings.ConsumerGroupName,
                settings.AzureEventHubsFullyQualifiedNamespace,
                settings.AzureEventHubsName,
                settings.AzureTokenCredential);
        }

        private BlobContainerClient GetBlobContainerClient(EventStreamConsumerSettings settings)
        {
            return new BlobContainerClient(new Uri(settings.AzureStorageBlobContainerUri), settings.AzureTokenCredential);
        }
    }
}
