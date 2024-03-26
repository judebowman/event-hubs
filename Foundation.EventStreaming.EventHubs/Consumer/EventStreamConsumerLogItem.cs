using System;

namespace Foundation.EventStreaming.EventHubs.Consumer
{
    public class EventStreamConsumerLogItem
    {
        public string EventId { get; set; }
        public string PartitionKey { get; set; }
        public string EventHubName { get; set; }
        public string ConsumerGroupName { get; set; }
        public string PartitionId { get; set; }
        public long SequenceNumber { get; set; }
        public long Offset { get; set; }
        public string EventContent { get; set; }
        public DateTimeOffset? EnqueuedTime { get; set; }
        public DateTime Timestamp { get; set; }
        public Exception Exception { get; set; }
        public string AdditionalInfo { get; set; }
    }
}
