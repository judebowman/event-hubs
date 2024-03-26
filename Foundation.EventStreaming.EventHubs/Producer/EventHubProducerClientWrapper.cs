using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace Foundation.EventStreaming.EventHubs.Producer
{
    public interface IEventHubProducerClient : IAsyncDisposable
    {
        Task SendAsync(IEnumerable<EventData> eventBatch, SendEventOptions options, CancellationToken cancellationToken = default);
        Task CloseAsync(CancellationToken cancellationToken = default);
        string FullyQualifiedNamespace { get; }
        string EventHubName { get; }
        bool IsClosed { get; }
    }

    internal class EventHubProducerClientWrapper : IEventHubProducerClient
    {
        private readonly EventHubProducerClient _eventHubProducerClient;

        public EventHubProducerClientWrapper(string fullyQualifiedNamespace, string eventHubName, TokenCredential tokenCredential)
        {
            var options = new EventHubProducerClientOptions
            {
                ConnectionOptions = new EventHubConnectionOptions
                {
                    TransportType = EventHubsTransportType.AmqpTcp
                },
                RetryOptions = new EventHubsRetryOptions
                {
                    Mode = EventHubsRetryMode.Exponential,
                    MaximumRetries = 3,
                    Delay = TimeSpan.FromSeconds(5),
                    MaximumDelay = TimeSpan.FromSeconds(30)
                }
            };

            _eventHubProducerClient = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, tokenCredential, options);
        }

        public string FullyQualifiedNamespace => _eventHubProducerClient.FullyQualifiedNamespace;
        public string EventHubName => _eventHubProducerClient.EventHubName;
        public bool IsClosed => _eventHubProducerClient.IsClosed;


        public Task SendAsync(IEnumerable<EventData> eventBatch, SendEventOptions options, CancellationToken cancellationToken = default)
        {
            return _eventHubProducerClient.SendAsync(eventBatch, options, cancellationToken);
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            return _eventHubProducerClient.CloseAsync(cancellationToken);
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
