using Azure.Core;
using FluentAssertions;
using Foundation.EventStreaming.EventHubs.Consumer;
using Foundation.EventStreaming.EventHubs.Producer;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.Extensibility;
using Ninject.MockingKernel.Moq;

namespace Foundation.EventStreaming.EventHubs.Tests;

/**
 * Note - the class under test uses a pair of static variables to keep track of if producers or consumers have already been created,
 * in order to throw an exception if AddLogging is called when producers or consumers have already been created.
 *
 * Running these tests in parallel would cause the tests to fail as the static variables would be shared between tests.
 * So the use of the Collection attribute is used to run the tests serially instead of in parallel.
 *
 * Normally with xUnit, you want to run tests in parallel to speed up the test run time, but in this case, we need to run the tests serially.
 **/

public class EventStreamFactoryTests
{
    private const string _azureEventHubsFullyQualifiedNamespace = "test-namespace";
    private const string _azureEventHubsName = "test-event-hub";

    private static EventStreamFactory GetEventStreamFactory(MoqMockingKernel autoMocker)
    {
        var dateTimeProvider = autoMocker.GetMock<IDateTimeProvider>().Object;
        var retryDelayWithExponentialBackoffProvider = autoMocker.GetMock<IRetryDelayWithExponentialBackoffProvider>().Object;
        var telemetryClient = GetTelemetryClient(new TelemetryChannelFake());
        var serviceProvider = autoMocker.GetMock<IServiceProvider>().Object;

        return new EventStreamFactory(
            dateTimeProvider, 
            retryDelayWithExponentialBackoffProvider, 
            telemetryClient, 
            serviceProvider);
    }

    private static TelemetryClient GetTelemetryClient(ITelemetryChannel telemetryChannel)
    {
        var telemetryConfiguration = new TelemetryConfiguration
        {
            TelemetryChannel = telemetryChannel,
            InstrumentationKey = Guid.NewGuid().ToString(),
        };
        return new TelemetryClient(telemetryConfiguration);
    }

    private static EventStreamConsumerSettings GetValidEventStreamConsumerSettings(MoqMockingKernel autoMocker)
    {
        return new EventStreamConsumerSettings
        {
            AzureEventHubsFullyQualifiedNamespace = _azureEventHubsFullyQualifiedNamespace,
            AzureEventHubsName = _azureEventHubsName,
            AzureTokenCredential = autoMocker.GetMock<TokenCredential>().Object,
            ConsumerGroupName = "test-consumer-group",
            AzureStorageBlobContainerUri = "https://test.blob.core.windows.net/consumer-checkpoint",
            AzureStorageBlogContainerName = "test-storage-blob-container-name"
        };
    }

    private static EventStreamProducerSettings GetValidEventStreamProducerSettings(MoqMockingKernel autoMocker)
    {
        return new EventStreamProducerSettings
        {
            AzureEventHubsFullyQualifiedNamespace = _azureEventHubsFullyQualifiedNamespace,
            AzureEventHubsName = _azureEventHubsName,
            AzureTokenCredential = autoMocker.GetMock<TokenCredential>().Object,
        };
    }

    public class When_calling_AddGlobalConsumerLogging
    {
        [Collection("Sequential")]
        public class When_a_consumer_has_not_already_been_created : TestBase
        {
            [Fact]
            public void Then_an_exception_should_not_be_thrown()
            {
                var exception = Record.Exception(() => { EventStreamFactory.AddGlobalConsumerLogging((_) => { }); });
                exception.Should().BeNull();
            }
        }

        [Collection("Sequential")]
        public class When_a_consumer_has_already_been_created : TestBase
        {
            private readonly EventStreamFactory _eventStreamFactory;

            public When_a_consumer_has_already_been_created()
            {
                _eventStreamFactory = GetEventStreamFactory(AutoMocker);
            }

            [Fact]
            public void Then_an_exception_should_be_thrown()
            {
                var settings = GetValidEventStreamConsumerSettings(AutoMocker);
                _eventStreamFactory.CreateConsumer<object>(settings);

                var exception = Record.Exception(() => { EventStreamFactory.AddGlobalConsumerLogging((_) => { }); });
                exception.Should().NotBeNull();
                exception!.Message.Should().Be("One or more consumers have already been created.  Calls to this method should be made during application start up before any consumers have been created.");
            }
        }
    }

    public class When_calling_AddGlobalProducerLogging
    {
        [Collection("Sequential")]
        public class When_a_producer_has_not_already_been_created : TestBase
        {
            [Fact]
            public void Then_an_exception_should_not_be_thrown()
            {
                var exception = Record.Exception(() => { EventStreamFactory.AddGlobalProducerLogging((_) => { }); });
                exception.Should().BeNull();
            }
        }

        [Collection("Sequential")]
        public class When_a_producer_has_already_been_created : TestBase
        {
            private readonly EventStreamFactory _eventStreamFactory;

            public When_a_producer_has_already_been_created()
            {
                _eventStreamFactory = GetEventStreamFactory(AutoMocker);
            }

            [Fact]
            public void Then_an_exception_should_be_thrown()
            {
                var settings = GetValidEventStreamProducerSettings(AutoMocker);
                _eventStreamFactory.CreateProducer(settings);

                var exception = Record.Exception(() => { EventStreamFactory.AddGlobalProducerLogging((_) => { }); });
                exception.Should().NotBeNull();
                exception!.Message.Should().Be("One or more producers have already been created.  Calls to this method should be made during application start up before any producers have been created.");
            }
        }
    }

    public class When_calling_CreateProducer
    {
        public class With_valid_settings
        {
            [Collection("Sequential")]
            public class When_a_producer_for_the_event_hub_has_not_already_been_created : TestBase
            {
                private readonly EventStreamFactory _eventStreamFactory;
                private readonly EventStreamProducerSettings _settings;

                public When_a_producer_for_the_event_hub_has_not_already_been_created()
                {
                    _eventStreamFactory = GetEventStreamFactory(AutoMocker);
                    _settings = GetValidEventStreamProducerSettings(AutoMocker);
                }

                [Fact]
                public void Then_it_should_return_an_instance_of_EventStreamProducer()
                {
                    var result = _eventStreamFactory.CreateProducer(_settings);

                    result.Should().BeOfType<EventStreamProducer>();
                    result.Should().NotBeNull();
                }
            }

            [Collection("Sequential")]
            public class When_a_producer_for_the_event_hub_has_already_been_created : TestBase
            {
                private readonly EventStreamFactory _eventStreamFactory;
                private readonly EventStreamProducerSettings _settings;

                public When_a_producer_for_the_event_hub_has_already_been_created()
                {
                    _eventStreamFactory = GetEventStreamFactory(AutoMocker);
                    _settings = GetValidEventStreamProducerSettings(AutoMocker);
                }

                [Fact]
                public void Then_it_should_return_the_same_instance_of_EventStreamProducer()
                {
                    var resultOne = _eventStreamFactory.CreateProducer(_settings);
                    var resultTwo = _eventStreamFactory.CreateProducer(_settings);

                    resultOne.Should().Be(resultTwo);
                }
            }
        }

        public class With_invalid_settings : TestBase
        {
            private readonly EventStreamFactory _eventStreamFactory;

            public With_invalid_settings()
            {
                _eventStreamFactory = GetEventStreamFactory(AutoMocker);
            }

            [Fact]
            public void Then_an_exception_should_be_thrown()
            {
                var settings = new EventStreamProducerSettings();
                var exception = Record.Exception(() => _eventStreamFactory.CreateProducer(settings));
                exception.Should().NotBeNull();
            }
        }
    }

    public class When_calling_CreateConsumer
    {
        [Collection("Sequential")]
        public class With_valid_settings : TestBase
        {
            private readonly EventStreamFactory _eventStreamFactory;

            public With_valid_settings()
            {
                _eventStreamFactory = GetEventStreamFactory(AutoMocker);
            }
            
            [Fact]
            public void Then_it_should_return_an_instance_of_EventStreamConsumer()
            {
                var settings = GetValidEventStreamConsumerSettings(AutoMocker);

                var result = _eventStreamFactory.CreateConsumer<object>(settings);

                result.Should().BeOfType<EventStreamConsumer<object>>();
                result.Should().NotBeNull();
            }
        }

        public class With_invalid_settings : TestBase
        {
            private readonly EventStreamFactory _eventStreamFactory;

            public With_invalid_settings()
            {
                _eventStreamFactory = GetEventStreamFactory(AutoMocker);
            }

            [Fact]
            public void Then_an_exception_should_be_thrown()
            {
                var settings = new EventStreamConsumerSettings();
                var exception = Record.Exception(() => _eventStreamFactory.CreateConsumer<object>(settings));
                exception.Should().NotBeNull();
            }
        }
    }
}