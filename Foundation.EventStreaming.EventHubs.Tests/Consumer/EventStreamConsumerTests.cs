using Azure.Core;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using FluentAssertions;
using Foundation.EventStreaming.EventHubs.Consumer;
using Foundation.EventStreaming.EventHubs.Exceptions;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Ninject.MockingKernel.Moq;

namespace Foundation.EventStreaming.EventHubs.Tests.Consumer;

public class EventStreamConsumerTests
{
    #region Shared Variables
    private const string _azureStorageBlobContainerUri = "https://test.blob.core.windows.net/";
    private const string _azureStorageBlogContainerName = "test-container";
    private const string _azureEventHubsFullyQualifiedNamespace = "test-namespace";
    private const string _azureEventHubsName = "test-event-hub";
    private const string _consumerGroupName = "test-consumer-group";
    private const string _partitionId = "4";
    private const string _partitionKey = "test-partition-key";
    private const long _offset = 11010;
    private const long _sequenceNumber = 230303;
    private static readonly DateTimeOffset _enqueuedTime = DateTimeOffset.Now;
    private static readonly Guid _messageId = Guid.Parse("5d8d9d40-700d-4d39-88d8-7de428fc8247");
    private static readonly Guid _messageIdTwo = Guid.Parse("5ed21d42-c547-49ae-be98-5a41fc633ba1");
    private const string _eventStreamFailureExceptionMessage = "A problem occured which causes OnEventStreamFailure to be raised";
    private const string _onFinalExceptionExceptionMessage = "A problem occured by a consumer implementation during OnFinalException";
    private const string _onEventReceivedExceptionMessage = "A problem occured by a consumer implementation during OnEventReceived";
    private const string _onEventStreamFailureExceptionMessage = "A problem occured by a consumer implementation during OnEventStreamFailure";
    #endregion

    #region Shared Helper Methods
    private static EventStreamConsumer<TEvent> CreateConsumer<TEvent>(
        MoqMockingKernel autoMocker, 
        TelemetryClient? telemetryClient = null, 
        int? numberOfRetries = null) where TEvent : class
    {
        var client = autoMocker.GetMock<IEventProcessorClient>().Object;
        var settings = GetEventStreamConsumerSettings(numberOfRetries);
        var dateTimeProvider = autoMocker.GetMock<IDateTimeProvider>().Object;
        var retryDelayProvider = autoMocker.GetMock<IRetryDelayWithExponentialBackoffProvider>().Object;
        var serviceProvider = autoMocker.GetMock<IServiceProvider>().Object;

        telemetryClient ??= GetTelemetryClient(new TelemetryChannelFake());

        return new EventStreamConsumer<TEvent>(client, settings, dateTimeProvider, retryDelayProvider, serviceProvider, telemetryClient);
    }

    private static EventStreamConsumerSettings GetEventStreamConsumerSettings(int? numberOfRetries = null)
    {
        return new EventStreamConsumerSettings
        {
            AzureTokenCredential = new Mock<TokenCredential>().Object,
            AzureStorageBlobContainerUri = _azureStorageBlobContainerUri,
            AzureStorageBlogContainerName = _azureStorageBlogContainerName,
            AzureEventHubsFullyQualifiedNamespace = _azureEventHubsFullyQualifiedNamespace,
            AzureEventHubsName = _azureEventHubsName,
            ConsumerGroupName = _consumerGroupName,
            SecondsBetweenCheckpointUpdatesOverride = 1,
            NumberOfRetriesOverride = numberOfRetries
        };
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

    private static ProcessEventArgs GetProcessEventArgs(Func<CancellationToken, Task>? updateCheckpointCallback = null, Guid? messageId = null)
    {
        var partitionContext = EventHubsModelFactory.PartitionContext(
            fullyQualifiedNamespace: _azureEventHubsFullyQualifiedNamespace,
            eventHubName: _azureEventHubsName,
            consumerGroup: _consumerGroupName,
            partitionId: _partitionId);

       var testEvent = new TestEventType { Name = "test" };

        var eventData = EventHubsModelFactory.EventData(
            eventBody: new BinaryData(testEvent),
            systemProperties: new Dictionary<string, object>(),
            partitionKey: _partitionKey,
            sequenceNumber: _sequenceNumber,
            offset: _offset,
            enqueuedTime: _enqueuedTime);

        eventData.MessageId = messageId.HasValue ? messageId.ToString() : _messageId.ToString();

        var processEventArgs = new ProcessEventArgs(
            partition: partitionContext,
            data: eventData,
            updateCheckpointImplementation: updateCheckpointCallback ?? (_ => Task.CompletedTask));

        return processEventArgs;
    }
    #endregion

    public class When_calling_StartProcessingAsync
    {
        public class When_consumer_is_already_running : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            public When_consumer_is_already_running()
            {
                _consumer = CreateConsumer<TestEventType>(AutoMocker);

                _consumer.OnEventReceived += (_, _) => Task.CompletedTask;
                _consumer.OnFinalException += (_, _, _) => Task.CompletedTask;
                _consumer.OnEventStreamFailure += _ => Task.CompletedTask;
            }

            [Fact]
            public async Task Then_should_throw_an_exception()
            {
                var cancellationTokenSource = new CancellationTokenSource();

                var exceptionOne = await Record.ExceptionAsync(async () => await _consumer.StartProcessingAsync(cancellationTokenSource));
                var exceptionTwo = await Record.ExceptionAsync(async () => await _consumer.StartProcessingAsync(cancellationTokenSource));

                exceptionOne.Should().BeNull();
                exceptionTwo.Should().NotBeNull();
            }
        }

        public class When_OnEventReceived_is_not_set : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;

            public When_OnEventReceived_is_not_set()
            {
                _consumer = CreateConsumer<TestEventType>(AutoMocker);
            }

            [Fact]
            public async Task Then_should_throw_an_exception()
            {
                var cancellationTokenSource = new CancellationTokenSource();

                var exception = await Record.ExceptionAsync(async () => await _consumer.StartProcessingAsync(cancellationTokenSource));

                exception.Should().NotBeNull();
                exception.Should().BeOfType<EventStreamSetupException>();
                exception!.Message.Should().Be("OnEventReceived must be set before starting the consumer.");
            }
        }

        public class When_OnFinalException_is_not_set : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;

            public When_OnFinalException_is_not_set()
            {
                _consumer = CreateConsumer<TestEventType>(AutoMocker);
                _consumer.OnEventReceived += (_, _) => Task.CompletedTask;
            }

            [Fact]
            public async Task Then_should_throw_an_exception()
            {
                var cancellationTokenSource = new CancellationTokenSource();

                var exception = await Record.ExceptionAsync(async () => await _consumer.StartProcessingAsync(cancellationTokenSource));

                exception.Should().NotBeNull();
                exception.Should().BeOfType<EventStreamSetupException>();
                exception!.Message.Should().Be("OnFinalException must be set before starting the consumer.");
            }
        }
        
        public class When_OnEventStreamFailure_is_not_set : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;

            public When_OnEventStreamFailure_is_not_set()
            {
                _consumer = CreateConsumer<TestEventType>(AutoMocker);
                _consumer.OnEventReceived += (_, _) => Task.CompletedTask;
                _consumer.OnFinalException += (_, _, _) => Task.CompletedTask;
            }

            [Fact]
            public async Task Then_should_throw_an_exception()
            {
                var cancellationTokenSource = new CancellationTokenSource();

                var exception = await Record.ExceptionAsync(async () => await _consumer.StartProcessingAsync(cancellationTokenSource));

                exception.Should().NotBeNull();
                exception.Should().BeOfType<EventStreamSetupException>();
                exception!.Message.Should().Be("OnEventStreamFailure must be set before starting the consumer.");
            }
        }
    }

    public class When_calling_StopProcessingAsync
    {
        public class And_no_exception_is_thrown : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            private readonly ProcessEventArgs _processEventArgs;
            private readonly TelemetryChannelFake _telemetryChannelFake;
            private int _numberOfOnEventReceivedInvocations;
            private int _numberOfUpdateCheckpointInvocations;

            public And_no_exception_is_thrown()
            {
                _telemetryChannelFake = new TelemetryChannelFake();
                var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient);
                _consumer.OnEventReceived += OnEventReceived;
                _consumer.OnFinalException += (_, _, _) => Task.CompletedTask;
                _consumer.OnEventStreamFailure += _ => Task.CompletedTask;

                _processEventArgs = GetProcessEventArgs(UpdateCheckpointCallback);

                SetupMocks();
            }

            private Task OnEventReceived(EventStreamData<TestEventType> arg1, IServiceScope arg2)
            {
                _numberOfOnEventReceivedInvocations++;
                return Task.CompletedTask;
            }

            private void SetupMocks()
            {
                SetupServiceScope();
            }

            private Task UpdateCheckpointCallback(CancellationToken cancellationToken)
            {
                _numberOfUpdateCheckpointInvocations++;
                return Task.CompletedTask;
            }

            [Fact]
            public async Task Then_no_new_events_should_be_processed()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);
                await _consumer.StopProcessingAsync();

                await AutoMocker.GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnEventReceivedInvocations.Should().Be(0);
            }

            [Fact]
            public async Task Then_the_checkpoint_should_be_saved()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker.GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                await _consumer.StopProcessingAsync();

                _numberOfUpdateCheckpointInvocations.Should().Be(1);
            }

            [Fact]
            public async Task Then_the_consumer_should_be_stopped()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await _consumer.StopProcessingAsync();

                AutoMocker.GetMock<IEventProcessorClient>()
                    .Verify(x => x.StopProcessingAsync(default), Times.Once);
            }

            [Fact]
            public async Task Then_the_telemetry_client_should_be_flushed()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await _consumer.StopProcessingAsync();

                _telemetryChannelFake.IsFlushed.Should().BeTrue();
            }
        }

        public class And_an_exception_is_thrown : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            private readonly TelemetryChannelFake _telemetryChannelFake;
            private const string _exceptionMessage = "An exception occurred while stopping the consumer";

            public And_an_exception_is_thrown()
            {
                _telemetryChannelFake = new TelemetryChannelFake();
                var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient);
                _consumer.OnEventReceived += (_, _) => Task.CompletedTask;
                _consumer.OnFinalException += (_, _, _) => Task.CompletedTask;
                _consumer.OnEventStreamFailure += _ => Task.CompletedTask;

                SetupMocks();
            }

            private void SetupMocks()
            {
                SetupServiceScope();

                AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .Setup(x => x.StopProcessingAsync(It.IsAny<CancellationToken>()))
                    .ThrowsAsync(new Exception(_exceptionMessage));
            }

            [Fact]
            public async Task Then_the_exception_should_be_logged()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                _ = await Record.ExceptionAsync(async () => await _consumer.StopProcessingAsync());

                _telemetryChannelFake
                    .SentExceptions
                    .Count(x => x.Exception.Message == _exceptionMessage)
                    .Should()
                    .Be(1);
            }

            [Fact]
            public async Task Then_the_exception_should_be_rethrown()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                var exception = await Record.ExceptionAsync(async () => await _consumer.StopProcessingAsync());

                exception.Should().NotBeNull();
                exception!.Message.Should().Be(_exceptionMessage);
            }

            [Fact]
            public async Task Then_the_telemetry_client_should_be_flushed()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                _ = await Record.ExceptionAsync(async () => await _consumer.StopProcessingAsync());

                _telemetryChannelFake.IsFlushed.Should().BeTrue();
            }
        }
    }

    public class When_events_are_received
    {
        public class With_no_exceptions_thrown : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            private readonly ProcessEventArgs _processEventArgs;
            private int _numberOfOnEventReceivedInvocations;
            private int _numberOfOnFinalExceptionInvocations;
            private int _numberOfUpdateCheckpointInvocations;
            private readonly TelemetryChannelFake _telemetryChannelFake;

            public With_no_exceptions_thrown()
            {
                _telemetryChannelFake = new TelemetryChannelFake();
                var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient);
                _consumer.OnEventReceived += OnEventReceived;
                _consumer.OnFinalException += OnFinalException;
                _consumer.OnEventStreamFailure += _ => Task.CompletedTask;
                
                _processEventArgs = GetProcessEventArgs(UpdateCheckpointCallback);

                SetupMocks();
            }

            private void SetupMocks()
            {
                SetupServiceScope();
            }

            private Task UpdateCheckpointCallback(CancellationToken cancellationToken)
            {
                _numberOfUpdateCheckpointInvocations++;
                return Task.CompletedTask;
            }

            private Task OnEventReceived(EventStreamData<TestEventType> eventStreamData, IServiceScope serviceScope)
            {
                _numberOfOnEventReceivedInvocations++;
                return Task.CompletedTask;
            }

            private Task OnFinalException(Exception arg1, ProcessEventArgs arg2, IServiceScope arg3)
            {
                _numberOfOnFinalExceptionInvocations++;
                return Task.CompletedTask;
            }

            [Fact]
            public async Task Then_OnEventReceived_should_be_invoked_once()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker.GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnEventReceivedInvocations.Should().Be(1);
            }

            [Fact]
            public async Task Then_OnFinalException_should_not_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker.GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnFinalExceptionInvocations.Should().Be(0);
            }

            [Fact]
            public async Task Then_UpdateCheckpointAsync_should_be_invoked_after_the_configured_wait_time()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker.GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                await Task.Delay(1000, default);

                _numberOfUpdateCheckpointInvocations.Should().Be(1);
            }

            [Fact]
            public async Task Then_RetryDelayProvider_should_not_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                AutoMocker
                    .GetMock<IRetryDelayWithExponentialBackoffProvider>()
                    .Verify(x => x.WaitForRetryDelay(It.IsAny<int>()), Times.Never);
            }

            [Fact]
            public async Task Then_the_correct_data_should_be_logged_to_the_TelemetryClient()
            { 
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                var telemetryOperation = _telemetryChannelFake.SentOperations.First();

                telemetryOperation.Name
                    .Should()
                    .Be(_azureEventHubsName);

                telemetryOperation
                    .Properties
                    .Should()
                    .BeEquivalentTo(new Dictionary<string, string>
                    {
                        { TelemetryPropertyNames.EventHubName, _azureEventHubsName },
                        { TelemetryPropertyNames.ConsumerGroupName, _consumerGroupName },
                        { TelemetryPropertyNames.PartitionId, _partitionId },
                        { TelemetryPropertyNames.Offset, _offset.ToString() },
                        { TelemetryPropertyNames.SequenceNumber, _sequenceNumber.ToString() },
                        { TelemetryPropertyNames.EnqueuedTime, _enqueuedTime.ToString() },
                        { TelemetryPropertyNames.MessageId, _messageId.ToString() }
                    });
            }
        }

        public class With_an_empty_event : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            private readonly ProcessEventArgs _processEventArgs;
            private int _numberOfOnEventReceivedInvocations;
            private int _numberOfOnFinalExceptionInvocations;
            private int _numberOfUpdateCheckpointInvocations;

            public With_an_empty_event()
            {
                _consumer = CreateConsumer<TestEventType>(AutoMocker);
                _consumer.OnEventReceived += OnEventReceived;
                _consumer.OnFinalException += OnFinalException;
                _consumer.OnEventStreamFailure += _ => Task.CompletedTask;

                _processEventArgs = GetProcessEventArgsWithEmptyData(UpdateCheckpointCallback);
            }

            private static ProcessEventArgs GetProcessEventArgsWithEmptyData(Func<CancellationToken, Task>? updateCheckpointCallback = null)
            {
                var partitionContext = EventHubsModelFactory.PartitionContext(
                    fullyQualifiedNamespace: _azureEventHubsFullyQualifiedNamespace,
                    eventHubName: _azureEventHubsName,
                    consumerGroup: _consumerGroupName,
                    partitionId: _partitionId);

                var processEventArgs = new ProcessEventArgs(
                    partition: partitionContext,
                    data: null,
                    updateCheckpointImplementation: updateCheckpointCallback ?? (_ => Task.CompletedTask));

                return processEventArgs;
            }

            private Task UpdateCheckpointCallback(CancellationToken arg)
            {
                _numberOfUpdateCheckpointInvocations++;
                return Task.CompletedTask;
            }

            private Task OnEventReceived(EventStreamData<TestEventType> arg1, IServiceScope arg2)
            {
                _numberOfOnEventReceivedInvocations++;
                return Task.CompletedTask;
            }

            private Task OnFinalException(Exception arg1, ProcessEventArgs arg2, IServiceScope arg3)
            {
                _numberOfOnFinalExceptionInvocations++;
                return Task.CompletedTask;
            }

            [Fact]
            public async Task Then_OnEventReceived_should_not_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnEventReceivedInvocations.Should().Be(0);
            }

            [Fact]
            public async Task Then_UpdateCheckpointAsync_should_not_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfUpdateCheckpointInvocations.Should().Be(0);
            }

            [Fact]
            public async Task Then_OnFinalException_should_not_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnFinalExceptionInvocations.Should().Be(0);
            }

            [Fact]
            public async Task Then_RetryDelayProvider_should_not_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                AutoMocker
                    .GetMock<IRetryDelayWithExponentialBackoffProvider>()
                    .Verify(x => x.WaitForRetryDelay(It.IsAny<int>()), Times.Never);
            }
        }

        public class With_an_event_that_cannot_be_deserialized : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            private readonly ProcessEventArgs _processEventArgs;
            private int _numberOfOnEventReceivedInvocations;
            private int _numberOfOnFinalExceptionInvocations;
            private int _numberOfUpdateCheckpointInvocations;
            private readonly TelemetryChannelFake _telemetryChannelFake;

            public With_an_event_that_cannot_be_deserialized()
            {
                _telemetryChannelFake = new TelemetryChannelFake();
                var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient);
                _consumer.OnEventReceived += OnEventReceived;
                _consumer.OnFinalException += OnFinalException;
                _consumer.OnEventStreamFailure += _ => Task.CompletedTask;

                _processEventArgs = GetProcessEventArgsWithInvalidType(UpdateCheckpointCallback);

                SetupMocks();
            }

            private void SetupMocks()
            {
                SetupServiceScope();
                //var mockCorrelationContext = AutoMocker.GetMock<ICorrelationContext>();
                //var mockServiceProvider = AutoMocker.GetMock<IServiceProvider>();
                //mockServiceProvider.Setup(x => x.GetService(typeof(ICorrelationContext))).Returns(mockCorrelationContext.Object);
            }

            private Task UpdateCheckpointCallback(CancellationToken cancellationToken)
            {
                _numberOfUpdateCheckpointInvocations++;
                return Task.CompletedTask;
            }

            private Task OnFinalException(Exception arg1, ProcessEventArgs arg2, IServiceScope arg3)
            {
                _numberOfOnFinalExceptionInvocations++;
                return Task.CompletedTask;
            }

            private Task OnEventReceived(EventStreamData<TestEventType> eventStreamData, IServiceScope serviceScope)
            {
                _numberOfOnEventReceivedInvocations++;
                return Task.CompletedTask;
            }

            private static ProcessEventArgs GetProcessEventArgsWithInvalidType(Func<CancellationToken, Task>? updateCheckpointCallback = null)
            {
                var partitionContext = EventHubsModelFactory.PartitionContext(
                    fullyQualifiedNamespace: _azureEventHubsFullyQualifiedNamespace,
                    eventHubName: _azureEventHubsName,
                    consumerGroup: _consumerGroupName,
                    partitionId: _partitionId);

                var eventData = EventHubsModelFactory.EventData(
                    eventBody: new BinaryData("invalid event data"),
                    systemProperties: new Dictionary<string, object>(),
                    partitionKey: _partitionKey,
                    sequenceNumber: _sequenceNumber,
                    offset: _offset,
                    enqueuedTime: _enqueuedTime);

                eventData.MessageId = _messageId.ToString();

                var processEventArgs = new ProcessEventArgs(
                    partition: partitionContext,
                    data: eventData,
                    updateCheckpointImplementation: updateCheckpointCallback ?? (_ => Task.CompletedTask));

                return processEventArgs;
            }
            
            [Fact]
            public async Task Then_RetryDelayProvider_should_not_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                AutoMocker
                    .GetMock<IRetryDelayWithExponentialBackoffProvider>()
                    .Verify(x => x.WaitForRetryDelay(It.IsAny<int>()), Times.Never);
            }

            [Fact]
            public async Task Then_OnEventReceived_should_not_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnEventReceivedInvocations.Should().Be(0);
            }

            [Fact]
            public async Task Then_OnFinalException_should_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnFinalExceptionInvocations.Should().Be(1);
            }

            [Fact]
            public async Task Then_UpdateCheckpointAsync_should_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                await Task.Delay(1000, default);

                _numberOfUpdateCheckpointInvocations.Should().Be(1);
            }

            [Fact]
            public async Task Then_the_correct_data_should_be_logged_to_the_TelemetryClient()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                var telemetryOperation = _telemetryChannelFake.SentOperations.First();

                telemetryOperation.Name
                    .Should()
                    .Be(_azureEventHubsName);

                telemetryOperation
                    .Properties
                    .Should()
                    .BeEquivalentTo(new Dictionary<string, string>
                    {
                        { TelemetryPropertyNames.EventHubName, _azureEventHubsName },
                        { TelemetryPropertyNames.ConsumerGroupName, _consumerGroupName },
                        { TelemetryPropertyNames.PartitionId, _partitionId },
                        { TelemetryPropertyNames.Offset, _offset.ToString() },
                        { TelemetryPropertyNames.SequenceNumber, _sequenceNumber.ToString() },
                        { TelemetryPropertyNames.EnqueuedTime, _enqueuedTime.ToString() },
                        { TelemetryPropertyNames.MessageId, _messageId.ToString() },
                    });
            }

            [Fact]
            public async Task Then_an_exception_should_be_logged_to_the_TelemetryClient()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _telemetryChannelFake
                    .SentExceptions
                    .Count()
                    .Should()
                    .Be(1);
            }
        }

        public class With_an_event_that_throws_a_transient_exception : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            private readonly ProcessEventArgs _processEventArgs;
            private int _numberOfOnEventReceivedInvocations;
            private int _numberOfOnFinalExceptionInvocations;
            private int _numberOfUpdateCheckpointInvocations;
            private readonly TelemetryChannelFake _telemetryChannelFake;

            public With_an_event_that_throws_a_transient_exception()
            {
                _telemetryChannelFake = new TelemetryChannelFake();
                var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient);
                _consumer.OnEventReceived += OnEventReceived;
                _consumer.OnFinalException += OnFinalException;
                _consumer.OnEventStreamFailure += _ => Task.CompletedTask;

                _processEventArgs = GetProcessEventArgs(UpdateCheckpointCallback);

                SetupMocks();
            }

            private void SetupMocks()
            {
                SetupServiceScope();
                //var mockCorrelationContext = AutoMocker.GetMock<ICorrelationContext>();
                //var mockServiceProvider = AutoMocker.GetMock<IServiceProvider>();
                //mockServiceProvider.Setup(x => x.GetService(typeof(ICorrelationContext))).Returns(mockCorrelationContext.Object);
            }

            private Task UpdateCheckpointCallback(CancellationToken cancellationToken)
            {
                _numberOfUpdateCheckpointInvocations++;
                return Task.CompletedTask;
            }

            private Task OnFinalException(Exception arg1, ProcessEventArgs arg2, IServiceScope arg3)
            {
                _numberOfOnFinalExceptionInvocations++;
                return Task.CompletedTask;
            }

            private Task OnEventReceived(EventStreamData<TestEventType> eventStreamData, IServiceScope serviceScope)
            {
                if (_numberOfOnEventReceivedInvocations < 1)
                {
                    _numberOfOnEventReceivedInvocations++;
                    throw new Exception(_onEventReceivedExceptionMessage);
                }
                _numberOfOnEventReceivedInvocations++;
                return Task.CompletedTask;
            }

            [Fact]
            public async Task Then_RetryDelayProvider_should_be_invoked_once()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                AutoMocker
                    .GetMock<IRetryDelayWithExponentialBackoffProvider>()
                    .Verify(x => x.WaitForRetryDelay(1), Times.Once);
            }


            [Fact]
            public async Task Then_OnEventReceived_should_be_invoked_twice()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnEventReceivedInvocations.Should().Be(2);
            }

            [Fact]
            public async Task Then_UpdateCheckpointAsync_should_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                await Task.Delay(1000, default);

                _numberOfUpdateCheckpointInvocations.Should().Be(1);
            }

            [Fact]
            public async Task Then_OnFinalException_should_not_be_invoked()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnFinalExceptionInvocations.Should().Be(0);
            }

            [Fact]
            public async Task Then_the_correct_data_should_be_logged_to_the_TelemetryClient()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                var telemetryOperation = _telemetryChannelFake.SentOperations.First();

                telemetryOperation.Name
                    .Should()
                    .Be(_azureEventHubsName);

                telemetryOperation
                    .Properties
                    .Should()
                    .BeEquivalentTo(new Dictionary<string, string>
                    {
                        { TelemetryPropertyNames.EventHubName, _azureEventHubsName },
                        { TelemetryPropertyNames.ConsumerGroupName, _consumerGroupName },
                        { TelemetryPropertyNames.PartitionId, _partitionId },
                        { TelemetryPropertyNames.Offset, _offset.ToString() },
                        { TelemetryPropertyNames.SequenceNumber, _sequenceNumber.ToString() },
                        { TelemetryPropertyNames.EnqueuedTime, _enqueuedTime.ToString() },
                        { TelemetryPropertyNames.MessageId, _messageId.ToString() },
                    });
            }

            [Fact]
            public async Task Then_an_exception_should_be_logged_to_the_TelemetryClient()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _telemetryChannelFake
                    .SentExceptions
                    .Count()
                    .Should()
                    .Be(1);
            }

        }

        public class With_an_event_that_throws_a_non_transient_exception
        {
            public class With_the_default_number_of_retries : TestBase
            {
                private readonly EventStreamConsumer<TestEventType> _consumer;
                private readonly ProcessEventArgs _processEventArgs;
                private int _numberOfOnEventReceivedInvocations;
                private int _numberOfOnFinalExceptionInvocations;
                private int _numberOfUpdateCheckpointInvocations;
                private readonly TelemetryChannelFake _telemetryChannelFake;

                public With_the_default_number_of_retries()
                {
                    _telemetryChannelFake = new TelemetryChannelFake();
                    var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                    _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient);
                    _consumer.OnEventReceived += OnEventReceived;
                    _consumer.OnFinalException += OnFinalException;
                    _consumer.OnEventStreamFailure += _ => Task.CompletedTask;

                    _processEventArgs = GetProcessEventArgs(UpdateCheckpointCallback);

                    SetupMocks();
                }

                private void SetupMocks()
                {
                    SetupServiceScope();
                    //var mockCorrelationContext = AutoMocker.GetMock<ICorrelationContext>();
                    //var mockServiceProvider = AutoMocker.GetMock<IServiceProvider>();
                    //mockServiceProvider.Setup(x => x.GetService(typeof(ICorrelationContext))).Returns(mockCorrelationContext.Object);
                }

                private Task UpdateCheckpointCallback(CancellationToken cancellationToken)
                {
                    _numberOfUpdateCheckpointInvocations++;
                    return Task.CompletedTask;
                }

                private Task OnFinalException(Exception arg1, ProcessEventArgs arg2, IServiceScope arg3)
                {
                    _numberOfOnFinalExceptionInvocations++;
                    return Task.CompletedTask;
                }

                private Task OnEventReceived(EventStreamData<TestEventType> eventStreamData, IServiceScope serviceScope)
                {
                    _numberOfOnEventReceivedInvocations++;
                    throw new Exception(_onEventReceivedExceptionMessage);
                }

                [Fact]
                public async Task Then_OnEventReceived_should_be_invoked_four_times()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    _numberOfOnEventReceivedInvocations.Should().Be(4);
                }

                [Fact]
                public async Task Then_OnFinalException_should_be_invoked_once()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    _numberOfOnFinalExceptionInvocations.Should().Be(1);
                }

                [Fact]
                public async Task Then_UpdateCheckpointAsync_should_be_invoked_once()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    await Task.Delay(1000, default);

                    _numberOfUpdateCheckpointInvocations.Should().Be(1);
                }

                [Fact]
                public void Then_the_number_of_retries_should_be_the_default_value()
                {
                    _consumer.NumberOfRetries.Should().Be(3);
                }

                [Fact]
                public async Task Then_RetryDelayProvider_should_be_invoked_three_times()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    AutoMocker
                        .GetMock<IRetryDelayWithExponentialBackoffProvider>()
                        .Verify(x => x.WaitForRetryDelay(1), Times.Once);

                    AutoMocker
                        .GetMock<IRetryDelayWithExponentialBackoffProvider>()
                        .Verify(x => x.WaitForRetryDelay(2), Times.Once);

                    AutoMocker
                        .GetMock<IRetryDelayWithExponentialBackoffProvider>()
                        .Verify(x => x.WaitForRetryDelay(3), Times.Once);
                }

                [Fact]
                public async Task Then_the_correct_data_should_be_logged_to_the_TelemetryClient()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    _telemetryChannelFake
                        .SentOperations
                        .Count()
                        .Should()
                        .Be(4);

                    foreach (var operationTelemetry in _telemetryChannelFake.SentOperations)
                    {
                        operationTelemetry.Name
                            .Should()
                            .Be(_azureEventHubsName);

                        operationTelemetry
                            .Properties
                            .Should()
                            .BeEquivalentTo(new Dictionary<string, string>
                            {
                                { TelemetryPropertyNames.EventHubName, _azureEventHubsName },
                                { TelemetryPropertyNames.ConsumerGroupName, _consumerGroupName },
                                { TelemetryPropertyNames.PartitionId, _partitionId },
                                { TelemetryPropertyNames.Offset, _offset.ToString() },
                                { TelemetryPropertyNames.SequenceNumber, _sequenceNumber.ToString() },
                                { TelemetryPropertyNames.EnqueuedTime, _enqueuedTime.ToString() },
                                { TelemetryPropertyNames.MessageId, _messageId.ToString() },
                            });
                    }
                }

                [Fact]
                public async Task Then_four_exceptions_should_be_logged_to_the_TelemetryClient()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    _telemetryChannelFake
                        .SentExceptions
                        .Count(x => x.Exception.Message == _onEventReceivedExceptionMessage)
                        .Should()
                        .Be(4);
                }
            }

            public class With_an_overridden_number_of_retries : TestBase
            {
                private readonly EventStreamConsumer<TestEventType> _consumer;
                private readonly ProcessEventArgs _processEventArgs;
                private int _numberOfOnEventReceivedInvocations;
                private int _numberOfOnFinalExceptionInvocations;
                private int _numberOfUpdateCheckpointInvocations;
                private readonly TelemetryChannelFake _telemetryChannelFake;
                private const int _expectedNumberOfRetries = 2;

                public With_an_overridden_number_of_retries()
                {
                    _telemetryChannelFake = new TelemetryChannelFake();
                    var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                    _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient, _expectedNumberOfRetries);
                    _consumer.OnEventReceived += OnEventReceived;
                    _consumer.OnFinalException += OnFinalException;
                    _consumer.OnEventStreamFailure += _ => Task.CompletedTask;

                    _processEventArgs = GetProcessEventArgs(UpdateCheckpointCallback);

                    SetupMocks();
                }

                private void SetupMocks()
                {
                    SetupServiceScope();
                }

                private Task UpdateCheckpointCallback(CancellationToken cancellationToken)
                {
                    _numberOfUpdateCheckpointInvocations++;
                    return Task.CompletedTask;
                }

                private Task OnFinalException(Exception arg1, ProcessEventArgs arg2, IServiceScope arg3)
                {
                    _numberOfOnFinalExceptionInvocations++;
                    return Task.CompletedTask;
                }

                private Task OnEventReceived(EventStreamData<TestEventType> eventStreamData, IServiceScope serviceScope)
                {
                    _numberOfOnEventReceivedInvocations++;
                    throw new Exception(_onEventReceivedExceptionMessage);
                }

                [Fact]
                public async Task Then_OnEventReceived_should_be_invoked_three_times()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    _numberOfOnEventReceivedInvocations.Should().Be(3);
                }

                [Fact]
                public async Task Then_OnFinalException_should_be_invoked_once()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    _numberOfOnFinalExceptionInvocations.Should().Be(1);
                }

                [Fact]
                public async Task Then_UpdateCheckpointAsync_should_be_invoked_once()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    await Task.Delay(1000, default);

                    _numberOfUpdateCheckpointInvocations.Should().Be(1);
                }

                [Fact]
                public void Then_the_number_of_retries_should_be_the_override_value()
                {
                    _consumer.NumberOfRetries.Should().Be(_expectedNumberOfRetries);
                }

                [Fact]
                public async Task Then_RetryDelayProvider_should_be_invoked_two_times()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    AutoMocker
                        .GetMock<IRetryDelayWithExponentialBackoffProvider>()
                        .Verify(x => x.WaitForRetryDelay(1), Times.Once);

                    AutoMocker
                        .GetMock<IRetryDelayWithExponentialBackoffProvider>()
                        .Verify(x => x.WaitForRetryDelay(2), Times.Once);
                }

                [Fact]
                public async Task Then_the_correct_data_should_be_logged_to_the_TelemetryClient()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    _telemetryChannelFake
                        .SentOperations
                        .Count()
                        .Should()
                        .Be(3);

                    foreach (var operationTelemetry in _telemetryChannelFake.SentOperations)
                    {
                        operationTelemetry.Name
                            .Should()
                            .Be(_azureEventHubsName);

                        operationTelemetry
                            .Properties
                            .Should()
                            .BeEquivalentTo(new Dictionary<string, string>
                            {
                                { TelemetryPropertyNames.EventHubName, _azureEventHubsName },
                                { TelemetryPropertyNames.ConsumerGroupName, _consumerGroupName },
                                { TelemetryPropertyNames.PartitionId, _partitionId },
                                { TelemetryPropertyNames.Offset, _offset.ToString() },
                                { TelemetryPropertyNames.SequenceNumber, _sequenceNumber.ToString() },
                                { TelemetryPropertyNames.EnqueuedTime, _enqueuedTime.ToString() },
                                { TelemetryPropertyNames.MessageId, _messageId.ToString() },
                            });
                    }
                }

                [Fact]
                public async Task Then_three_exceptions_should_be_logged_to_the_TelemetryClient()
                {
                    var cancellationTokenSource = new CancellationTokenSource();
                    await _consumer.StartProcessingAsync(cancellationTokenSource);

                    await AutoMocker
                        .GetMock<IEventProcessorClient>()
                        .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                    _telemetryChannelFake
                        .SentExceptions
                        .Count(x => x.Exception.Message == _onEventReceivedExceptionMessage)
                        .Should()
                        .Be(3);
                }
            }
        }

        public class With_an_event_that_throws_a_non_transient_exception_and_OnFinalException_throws_an_exception : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            private readonly ProcessEventArgs _processEventArgs;
            private int _numberOfOnEventReceivedInvocations;
            private int _numberOfOnFinalExceptionInvocations;
            private int _numberOfUpdateCheckpointInvocationsForEventWhichDoesNotThrowExceptions;
            private int _numberOfUpdateCheckpointInvocationsForEventWhichDoesThrowExceptions;
            
            private readonly TelemetryChannelFake _telemetryChannelFake;

            public With_an_event_that_throws_a_non_transient_exception_and_OnFinalException_throws_an_exception()
            {
                _telemetryChannelFake = new TelemetryChannelFake();
                var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient);
                _consumer.OnEventReceived += OnEventReceived;
                _consumer.OnFinalException += OnFinalException;
                _consumer.OnEventStreamFailure += _ => Task.CompletedTask;

                _processEventArgs = GetProcessEventArgs(UpdateCheckpointCallbackForEventWhichThrowsException);

                SetupMocks();
            }

            private void SetupMocks()
            {
                SetupServiceScope();
                //var mockCorrelationContext = AutoMocker.GetMock<ICorrelationContext>();
                //var mockServiceProvider = AutoMocker.GetMock<IServiceProvider>();
                //mockServiceProvider.Setup(x => x.GetService(typeof(ICorrelationContext))).Returns(mockCorrelationContext.Object);
            }

            private Task UpdateCheckpointCallbackForEventWhichDoesNotThrowException(CancellationToken cancellationToken)
            {
                _numberOfUpdateCheckpointInvocationsForEventWhichDoesNotThrowExceptions++;
                return Task.CompletedTask;
            }

            private Task UpdateCheckpointCallbackForEventWhichThrowsException(CancellationToken cancellationToken)
            {
                _numberOfUpdateCheckpointInvocationsForEventWhichDoesThrowExceptions++;
                return Task.CompletedTask;
            }

            private Task OnFinalException(Exception exception, ProcessEventArgs processEventArgs, IServiceScope serviceScope)
            {
                _numberOfOnFinalExceptionInvocations++;
                if (processEventArgs.Data.MessageId == _messageId.ToString())
                {
                    throw new Exception(_onFinalExceptionExceptionMessage);
                }

                return Task.CompletedTask;
            }

            private Task OnEventReceived(EventStreamData<TestEventType> eventStreamData, IServiceScope serviceScope)
            {
                _numberOfOnEventReceivedInvocations++;
                if (eventStreamData.EventId == _messageId)
                {
                    throw new Exception(_onEventReceivedExceptionMessage);
                }
                return Task.CompletedTask;
            }

            [Fact]
            public async Task Then_OnFinalException_should_be_invoked_once()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnFinalExceptionInvocations.Should().Be(1);
            }

            [Fact]
            public async Task Then_UpdateCheckpointAsync_should_be_invoked_once_for_the_event_which_does_not_throw_exceptions()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                var eventWhichDoesNotResultInAnExceptionBeingThrown = GetProcessEventArgs(UpdateCheckpointCallbackForEventWhichDoesNotThrowException, _messageIdTwo);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, eventWhichDoesNotResultInAnExceptionBeingThrown);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                await Task.Delay(1000, default);

                _numberOfUpdateCheckpointInvocationsForEventWhichDoesNotThrowExceptions.Should().Be(1);
            }

            [Fact]
            public async Task Then_UpdateCheckpointAsync_should_not_be_invoked_for_the_event_which_does_throw_exceptions()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                var eventWhichDoesNotResultInAnExceptionBeingThrown = GetProcessEventArgs(UpdateCheckpointCallbackForEventWhichDoesNotThrowException, _messageIdTwo);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, eventWhichDoesNotResultInAnExceptionBeingThrown);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                await Task.Delay(1000, default);

                _numberOfUpdateCheckpointInvocationsForEventWhichDoesThrowExceptions.Should().Be(0);
            }

            [Fact]
            public async Task Then_new_events_should_not_be_processed()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnEventReceivedInvocations.Should().Be(4);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _numberOfOnEventReceivedInvocations.Should().Be(4);
            }

            [Fact]
            public async Task Then_the_cancellation_should_be_requested_on_the_CancellationTokenSource()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                cancellationTokenSource.IsCancellationRequested.Should().BeTrue();
            }

            [Fact]
            public async Task Then_five_exceptions_should_be_logged_to_the_TelemetryClient()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _telemetryChannelFake
                    .SentExceptions
                    .Count()
                    .Should()
                    .Be(5);
            }

            [Fact]
            public async Task Then_the_exception_thrown_in_OnFinalException_should_be_logged_to_the_TelemetryClient()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessEventAsync += null, _processEventArgs);

                _telemetryChannelFake
                    .SentExceptions
                    .Count(x => x.Exception.Message == _onFinalExceptionExceptionMessage)
                    .Should()
                    .Be(1);
            }
        }
    }

    public class When_OnEventStreamFailure_is_raised : TestBase
    {
        public class With_the_event_handled_without_an_exception_being_thrown : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            private int _numberOfOnEventStreamFailureInvocations;
            private readonly Exception _exceptionRaisedInOnEventStreamFailure;
            private readonly TelemetryChannelFake _telemetryChannelFake;

            public With_the_event_handled_without_an_exception_being_thrown()
            {
                _telemetryChannelFake = new TelemetryChannelFake();
                var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient);
                _consumer.OnEventReceived += (_, _) => Task.CompletedTask;
                _consumer.OnFinalException += (_, _, _) => Task.CompletedTask;
                _consumer.OnEventStreamFailure += OnEventStreamFailure;

                _exceptionRaisedInOnEventStreamFailure = new Exception(_eventStreamFailureExceptionMessage);
            }

            private Task OnEventStreamFailure(ProcessErrorEventArgs arg)
            {
                _numberOfOnEventStreamFailureInvocations++;
                return Task.CompletedTask;
            }

            [Fact]
            public async Task Then_OnEventStreamFailure_should_be_invoked_once()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessErrorAsync += null,
                        new ProcessErrorEventArgs(
                            _partitionId,
                            "operation",
                            _exceptionRaisedInOnEventStreamFailure));

                _numberOfOnEventStreamFailureInvocations.Should().Be(1);
            }

            [Fact]
            public async Task Then_the_EventStreamFailure_exception_should_be_logged_to_the_TelemetryClient()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessErrorAsync += null,
                        new ProcessErrorEventArgs(
                            _partitionId,
                            "operation",
                            _exceptionRaisedInOnEventStreamFailure));

                _telemetryChannelFake
                    .SentExceptions
                    .Count(x => x.Exception.Message == _eventStreamFailureExceptionMessage)
                    .Should()
                    .Be(1);
            }
        }

        public class With_the_event_handled_with_an_exception_being_thrown : TestBase
        {
            private readonly EventStreamConsumer<TestEventType> _consumer;
            private int _numberOfOnEventStreamFailureInvocations;
            private readonly Exception _exceptionRaisedInOnEventStreamFailure;
            private readonly TelemetryChannelFake _telemetryChannelFake;

            public With_the_event_handled_with_an_exception_being_thrown()
            {
                _telemetryChannelFake = new TelemetryChannelFake();
                var telemetryClient = GetTelemetryClient(_telemetryChannelFake);

                _consumer = CreateConsumer<TestEventType>(AutoMocker, telemetryClient);
                _consumer.OnEventReceived += (_, _) => Task.CompletedTask;
                _consumer.OnFinalException += (_, _, _) => Task.CompletedTask;
                _consumer.OnEventStreamFailure += OnEventStreamFailure;

                _exceptionRaisedInOnEventStreamFailure = new Exception(_eventStreamFailureExceptionMessage);
            }

            private Task OnEventStreamFailure(ProcessErrorEventArgs arg)
            {
                _numberOfOnEventStreamFailureInvocations++;
                throw new Exception(_onEventStreamFailureExceptionMessage);
            }

            [Fact]
            public async Task Then_OnEventStreamFailure_should_be_invoked_once()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessErrorAsync += null,
                        new ProcessErrorEventArgs(
                            _partitionId,
                            "operation",
                            _exceptionRaisedInOnEventStreamFailure));

                _numberOfOnEventStreamFailureInvocations.Should().Be(1);
            }

            [Fact]
            public async Task Then_the_EventStreamFailure_exception_should_be_logged_to_the_TelemetryClient()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessErrorAsync += null,
                        new ProcessErrorEventArgs(
                            _partitionId,
                            "operation",
                            _exceptionRaisedInOnEventStreamFailure));

                _telemetryChannelFake
                    .SentExceptions
                    .Count(x => x.Exception.Message == _eventStreamFailureExceptionMessage)
                    .Should()
                    .Be(1);
            }

            [Fact]
            public async Task Then_the_exception_thrown_in_OnEventStreamFailure_should_be_logged_to_the_TelemetryClient()
            {
                var cancellationTokenSource = new CancellationTokenSource();
                await _consumer.StartProcessingAsync(cancellationTokenSource);

                await AutoMocker
                    .GetMock<IEventProcessorClient>()
                    .RaiseAsync(x => x.ProcessErrorAsync += null,
                        new ProcessErrorEventArgs(
                            _partitionId,
                            "operation",
                            _exceptionRaisedInOnEventStreamFailure));

                _telemetryChannelFake
                    .SentExceptions
                    .Count(x => x.Exception.Message == _onEventStreamFailureExceptionMessage)
                    .Should()
                    .Be(1);
            }
        }
    }
}

public class TestEventType
{
    public string Name { get; set; }
}