using System;
using System.Collections.Generic;

namespace Foundation.EventStreaming.EventHubs
{
    public class EventStreamData<TEvent>
    {
        public EventStreamData(Guid eventId, TEvent eventData) : this(eventId, eventData, null)
        {
        }

        public EventStreamData(Guid eventId, TEvent eventData, IDictionary<string, object> properties = null)
        {
            if (eventId == Guid.Empty)
            {
                throw new ArgumentNullException(nameof(eventId), "EventId cannot be empty");
            }

            EventId = eventId;
            EventData = eventData;
            Properties = properties;
        }

        public Guid EventId { get; set; }
        public TEvent EventData { get; set; }
        public IDictionary<string, object> Properties { get; set; }
    }
}
