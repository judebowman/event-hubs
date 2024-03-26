using System;

namespace Foundation.EventStreaming.EventHubs.Consumer
{
    public interface IDateTimeProvider
    {
        DateTime Now { get; }
    }
    public class DateTimeProvider : IDateTimeProvider
    {
        public DateTime Now => DateTime.Now;
    }
}
