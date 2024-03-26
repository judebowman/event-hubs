using System;
using Azure.Core;
using Foundation.EventStreaming.EventHubs.Exceptions;

namespace Foundation.EventStreaming.EventHubs.Producer
{
    public class EventStreamProducerSettings
    {
        public string AzureEventHubsFullyQualifiedNamespace { get; set; }
        public TokenCredential AzureTokenCredential { get; set; }
        public string AzureEventHubsName { get; set; }
        public Action<EventStreamProducerLogItem> LogAction { get; set; }

        public void Validate()
        {
            if (string.IsNullOrEmpty(AzureEventHubsFullyQualifiedNamespace))
            {
                throw new EventStreamSetupException("AzureEventHubsFullyQualifiedNamespace is required.");
            }

            if (AzureTokenCredential == null)
            {
                throw new EventStreamSetupException("AzureTokenCredential is required.");
            }

            if (string.IsNullOrWhiteSpace(AzureEventHubsName))
            {
                throw new EventStreamSetupException("AzureEventHubsName is required.");
            }
        }
    }
}
