using System;

namespace Foundation.EventStreaming.EventHubs.Exceptions
{
    public class EventStreamSetupException : Exception
    {
        public EventStreamSetupException(string message) : base(message)
        {
        }

        public EventStreamSetupException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
