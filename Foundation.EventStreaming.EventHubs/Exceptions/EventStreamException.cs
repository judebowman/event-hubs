using System;

namespace Foundation.EventStreaming.EventHubs.Exceptions
{
    public class EventStreamException : Exception
    {
        public EventStreamException(string message) : base(message)
        {
        }

        public EventStreamException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
