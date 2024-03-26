using System;
using Azure.Messaging.EventHubs.Processor;

namespace Foundation.EventStreaming.EventHubs.Consumer
{
    internal class TelemetryTrackingExceptionInfo
    {
        public ProcessEventArgs? ProcessEventArgs { get; set; }
        public ProcessErrorEventArgs? ProcessErrorEventArgs { get; set; }
        public Exception Exception { get; set; }
        public bool? IsPoisonEvent { get; set; }
        public bool? IsDeserializationException { get; set; }
        public bool? IsFinalException { get; set; }
        public bool? IsCriticalException { get; set; }
    }
}
