using System;
using System.Threading.Tasks;

namespace Foundation.EventStreaming.EventHubs.Consumer
{
    public interface IRetryDelayWithExponentialBackoffProvider
    {
        Task WaitForRetryDelay(int retryCount);
    }

    public class RetryDelayDelayWithExponentialBackoffProvider : IRetryDelayWithExponentialBackoffProvider
    {
        private readonly IDelayProvider _delayProvider;
        private const int _maximumRetryWaitTimeInMilliseconds = 64000;

        public RetryDelayDelayWithExponentialBackoffProvider(IDelayProvider delayProvider)
        {
            _delayProvider = delayProvider;
        }

        public async Task WaitForRetryDelay(int retryCount)
        {
            var retryWaitTimeInMilliseconds = GetRetryWaitTimeInMilliseconds(retryCount);
            var delayTimespan = TimeSpan.FromMilliseconds(retryWaitTimeInMilliseconds);
            await _delayProvider.Delay(delayTimespan);
        }

        private double GetRetryWaitTimeInMilliseconds(int retryCount)
        {
            // Avoid doing math if the retry count will exceed the maximum wait time
            if (retryCount >= 6)
            {
                return _maximumRetryWaitTimeInMilliseconds;
            }
            var randomNumberMilliseconds = new Random().Next(1, 1000);
            var exponentialBackoff = Math.Pow(2, retryCount) * 1000 + randomNumberMilliseconds;
            return Math.Min(exponentialBackoff, _maximumRetryWaitTimeInMilliseconds);
        }
    }
}
