using System.Collections;
using System.Globalization;
using FluentAssertions;
using Foundation.EventStreaming.EventHubs.Consumer;
using Moq;
using Ninject.MockingKernel.Moq;
using Xunit.Abstractions;

namespace Foundation.EventStreaming.EventHubs.Tests.Consumer;

public class RetryDelayDelayWithExponentialBackoffProviderTests
{
    private static RetryDelayDelayWithExponentialBackoffProvider GetRetryDelayWithExponentialBackoffProvider(MoqMockingKernel autoMocker)
    {
        var delayProvider = autoMocker.GetMock<IDelayProvider>();
        return new RetryDelayDelayWithExponentialBackoffProvider(delayProvider.Object);
    }

    public class When_calling_WaitForRetryDelay : TestBase
    {
        private readonly RetryDelayDelayWithExponentialBackoffProvider provider;
        private TimeSpan _submittedTimeSpan;
        private readonly ITestOutputHelper _testOutputHelper;

        public When_calling_WaitForRetryDelay(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            provider = GetRetryDelayWithExponentialBackoffProvider(AutoMocker);
            SetupMocks();
        }

        private void SetupMocks()
        {
            AutoMocker
                .GetMock<IDelayProvider>()
                .Setup(x => x.Delay(It.IsAny<TimeSpan>()))
                .Callback((TimeSpan t) => _submittedTimeSpan = t);
        }

        [Theory]
        [ClassData(typeof(RetryDelayTestData))]
        public async Task Should_call_DelayProvider_with_correct_timespan_when_the_input_parameter_is_one(int retryCount, double minValue, double maxValue)
        {
            await provider.WaitForRetryDelay(retryCount);
            _testOutputHelper.WriteLine(_submittedTimeSpan.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
            _submittedTimeSpan.TotalMilliseconds.Should().BeGreaterOrEqualTo(minValue);
            _submittedTimeSpan.TotalMilliseconds.Should().BeLessOrEqualTo(maxValue);
        }
    }

    public class RetryDelayTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { 1, 2000, 3000 };
            yield return new object[] { 2, 4000, 5000 };
            yield return new object[] { 3, 8000, 9000 };
            yield return new object[] { 4, 16000, 17000 };
            yield return new object[] { 5, 32000, 33000 };
            yield return new object[] { 6, 64000, 64000 };
            yield return new object[] { 7, 64000, 64000 };
            yield return new object[] { 8, 64000, 64000 };
            yield return new object[] { 9, 64000, 64000 };
            yield return new object[] { 10, 64000, 64000 };
            yield return new object[] { 100, 64000, 64000 };
            yield return new object[] { 1000, 64000, 64000 };
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

