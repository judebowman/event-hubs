using System;
using Azure.Core;
using Foundation.EventStreaming.EventHubs.Exceptions;

namespace Foundation.EventStreaming.EventHubs.Consumer
{
    public class EventStreamConsumerSettings
    {
        public string AzureStorageBlobContainerUri { get; set; }
        public string AzureStorageBlogContainerName { get; set; }
        public TokenCredential AzureTokenCredential { get; set; }
        public string AzureEventHubsFullyQualifiedNamespace { get; set; }
        public string AzureEventHubsName { get; set; }
        public string ConsumerGroupName { get; set; }
        public int? NumberOfRetriesOverride { get; set; }
        public int? SecondsBetweenCheckpointUpdatesOverride { get; set; }
        public Action<EventStreamConsumerLogItem> LogAction { get; set; }

        public void Validate()
        {
            ValidateAzureStorageSettings();
            ValidateAzureEventHubsSettings();
        }

        private void ValidateAzureStorageSettings()
        {
            if (string.IsNullOrEmpty(AzureStorageBlobContainerUri))
            {
                throw new EventStreamSetupException("AzureStorageBlobContainerUri is required.");
            }

            if (string.IsNullOrEmpty(AzureStorageBlogContainerName))
            {
                throw new EventStreamSetupException("AzureStorageBlogContainerName is required.");
            }

            if (AzureTokenCredential == null)
            {
                throw new EventStreamSetupException("AzureTokenCredential is required");
            }
        }

        private void ValidateAzureEventHubsSettings()
        {
            if (string.IsNullOrEmpty(AzureEventHubsFullyQualifiedNamespace))
            {
                throw new EventStreamSetupException("AzureEventHubsFullyQualifiedNamespace is required.");
            }
            
            if (string.IsNullOrWhiteSpace(AzureEventHubsName))
            {
                throw new EventStreamSetupException("AzureEventHubsName is required.");
            }

            if (string.IsNullOrWhiteSpace(ConsumerGroupName))
            {
                throw new EventStreamSetupException("ConsumerGroupName is required.");
            }
        }
    }
}
