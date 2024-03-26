using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Foundation.EventStreaming.EventHubs.Consumer;

namespace Foundation.EventStreaming.EventHubs.Producer
{
    public interface IEventStreamProducer : IAsyncDisposable
    {
        Task SendAsync<TEvent>(EventStreamData<TEvent> eventStreamData, string partitionKey = null, CancellationToken cancellationToken = default);
        Task SendAsync<TEvent>(IList<EventStreamData<TEvent>> eventList, string partitionKey = null, CancellationToken cancellationToken = default);
        Task CloseAsync();
    }

    public class EventStreamProducer : IEventStreamProducer
    {
        private readonly IEventHubProducerClient _eventHubProducerClient;
        private readonly IDateTimeProvider _dateTimeProvider;
        private Action<EventStreamProducerLogItem> _logAction;

        public EventStreamProducer(IEventHubProducerClient eventHubProducerClient, IDateTimeProvider dateTimeProvider)
        {
            ValidateConstructorParameters(eventHubProducerClient, dateTimeProvider);

            _eventHubProducerClient = eventHubProducerClient;
            _dateTimeProvider = dateTimeProvider;
        }

        private static void ValidateConstructorParameters(IEventHubProducerClient eventHubProducerClient, IDateTimeProvider dateTimeProvider)
        {
            if (eventHubProducerClient == null)
            {
                throw new ArgumentNullException(nameof(eventHubProducerClient));
            }

            if (dateTimeProvider == null)
            {
                throw new ArgumentNullException(nameof(dateTimeProvider));
            }
        }

        public Task SendAsync<TEvent>(EventStreamData<TEvent> eventStreamData, string partitionKey = null, CancellationToken cancellationToken = default)
        {
            return SendAsync(new[] { eventStreamData }, partitionKey, cancellationToken);
        }

        public async Task SendAsync<TEvent>(IList<EventStreamData<TEvent>> eventList, string partitionKey = null, CancellationToken cancellationToken = default)
        {
            var eventBatch = new List<EventData>();

            var sendEventOptions = new SendEventOptions
            {
                PartitionKey = partitionKey
            };

            var logItems = new List<EventStreamProducerLogItem>();

            foreach (var eventStreamData in eventList)
            {
                var messageByteArray = JsonSerializer.SerializeToUtf8Bytes(eventStreamData.EventData);
                var eventData = new EventData(BinaryData.FromBytes(messageByteArray))
                {
                    MessageId = eventStreamData.EventId.ToString()
                };

                if (eventStreamData.Properties != null)
                {
                    foreach (var property in eventStreamData.Properties)
                    {
                        eventData.Properties.Add(property);
                    }
                }

                eventBatch.Add(eventData);
                if (_logAction != null)
                {
                    var logItem = new EventStreamProducerLogItem
                    {
                        EventId = eventData.MessageId,
                        PartitionKey = sendEventOptions.PartitionKey,
                        Timestamp = _dateTimeProvider.Now,
                        EventHubName =  _eventHubProducerClient.EventHubName,
                        EventContent = Encoding.UTF8.GetString(messageByteArray)
                    };
                    logItems.Add(logItem);
                }
            }
            await _eventHubProducerClient.SendAsync(eventBatch, sendEventOptions, cancellationToken);

            LogEvents(logItems);
        }

        public async Task CloseAsync()
        {
            if (_eventHubProducerClient != null)
            {
                await _eventHubProducerClient.CloseAsync();
            }
        }

        private void LogEvents(List<EventStreamProducerLogItem> logItems)
        {
            try
            {
                if (_logAction != null)
                {
                    logItems.ForEach(logItem => _logAction(logItem));
                }
            }
            catch
            {
                // do nothing if logging fails
            }
        }

        public void AddLogging(Action<EventStreamProducerLogItem> logAction)
        {
            _logAction += logAction;
        }

        public async ValueTask DisposeAsync()
        {
            if (_eventHubProducerClient != null)
            {
                await _eventHubProducerClient.DisposeAsync();
            }
        }
    }
}
