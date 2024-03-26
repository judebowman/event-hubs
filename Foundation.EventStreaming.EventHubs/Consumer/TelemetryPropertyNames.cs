namespace Foundation.EventStreaming.EventHubs.Consumer
{
    public static class TelemetryPropertyNames
    {
        public const string EventHubName = "Event Hub Name";
        public const string ConsumerGroupName = "Consumer Group Name";
        public const string PartitionId = "Partition Id";
        public const string SequenceNumber = "Sequence Number";
        public const string Offset = "Offset";
        public const string EnqueuedTime = "Enqueued Time";
        public const string MessageId = "Message Id";
        public const string Operation = "Operation";
        public const string ExceptionMessage = "Exception Message";
        public const string IsEventProcessorClientError = "Is Event Processor Client Error";
        public const string IsPoisonEvent = "Is Poison Event";
        public const string IsDeserializationException = "Is Deserialization Exception";
        public const string IsCriticalException = "Is Critical Exception";
    }
}
