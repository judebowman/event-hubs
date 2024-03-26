using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace Foundation.EventStreaming.EventHubs.Consumer
{
    public interface IEventProcessorClient
    {
        Task StartProcessingAsync(CancellationToken cancellationToken = default);
        Task StopProcessingAsync(CancellationToken cancellationToken = default);

        event Func<ProcessEventArgs, Task> ProcessEventAsync;
        event Func<ProcessErrorEventArgs, Task> ProcessErrorAsync;
    }

    internal class EventProcessorClientWrapper : IEventProcessorClient
    {
        private readonly EventProcessorClient _eventProcessorClient;

        public EventProcessorClientWrapper(BlobContainerClient checkpointStore, string consumerGroupName, string fullyQualifiedNamespace, string eventHubName, TokenCredential tokenCredential)
        {
            _eventProcessorClient = new EventProcessorClient(checkpointStore, consumerGroupName, fullyQualifiedNamespace, eventHubName, tokenCredential);
        }

        public Task StartProcessingAsync(CancellationToken cancellationToken = default)
        {
            return _eventProcessorClient.StartProcessingAsync(cancellationToken);
        }

        public Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            return _eventProcessorClient.StopProcessingAsync(cancellationToken);
        }

        public event Func<ProcessEventArgs, Task> ProcessEventAsync
        {
            add => _eventProcessorClient.ProcessEventAsync += value;
            remove => _eventProcessorClient.ProcessEventAsync -= value;
        }

        public event Func<ProcessErrorEventArgs, Task> ProcessErrorAsync
        {
            add => _eventProcessorClient.ProcessErrorAsync += value;
            remove => _eventProcessorClient.ProcessErrorAsync -= value;
        }
    }
}
