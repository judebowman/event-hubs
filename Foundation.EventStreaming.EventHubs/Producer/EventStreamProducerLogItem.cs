using System;

namespace Foundation.EventStreaming.EventHubs.Producer
{
    public class EventStreamProducerLogItem
    {
        public string EventId { get; set; }
        public string PartitionKey { get; set; }
        public string EventHubName { get; set; }
        public string EventContent { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
