using System;
using System.Threading.Tasks;

namespace Foundation.EventStreaming.EventHubs.Consumer
{
    public interface IDelayProvider
    {
        Task Delay(TimeSpan delay);
    }

    public class DelayProvider : IDelayProvider
    {
        public Task Delay(TimeSpan delay)
        {
            return Task.Delay(delay);
        }
    }
}
