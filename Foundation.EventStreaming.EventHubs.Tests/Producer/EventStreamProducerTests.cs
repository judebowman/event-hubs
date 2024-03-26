using System.Text;
using System.Text.Json;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using FluentAssertions;
using Foundation.EventStreaming.EventHubs.Consumer;
using Foundation.EventStreaming.EventHubs.Producer;
using Foundation.EventStreaming.EventHubs.Tests.Consumer;
using Moq;
using Ninject.MockingKernel.Moq;

namespace Foundation.EventStreaming.EventHubs.Tests.Producer;

public class EventStreamProducerTests
{
    private static EventStreamProducer GetEventStreamProducer(MoqMockingKernel autoMocker)
    {
        return new EventStreamProducer(
            autoMocker.GetMock<IEventHubProducerClient>().Object,
            autoMocker.GetMock<IDateTimeProvider>().Object);
    }

    private static EventStreamData<TestEventType> GetTestEvent(Guid eventId, string name, IDictionary<string, object> properties)
    {
        return new EventStreamData<TestEventType>(eventId, new TestEventType { Name = name }, properties);
    }

    private const string _eventHubName = "eventHubName";

    private static readonly Guid _eventIdOne = Guid.Parse("23e4dcd9-0b60-4f36-a223-1a73edc47f39");
    private const string _eventNameOne = "One";
    private const string _propertyKeyOne = "keyOne";
    private const string _propertyValueOne = "valueOne";

    private static readonly Guid _eventIdTwo = Guid.Parse("54c874c2-a904-4942-8a84-04e5a24fd5df");
    private const string _eventNameTwo = "Two";
    private const string _propertyKeyTwo = "keyTwo";
    private const string _propertyValueTwo = "valueTwo";

    private const string _partitionKey = "PartitionKey";

    public class When_calling_SendAsync_with_single_event
    {
        public class Without_a_LogAction : TestBase
        {
            private readonly EventStreamProducer _eventStreamProducer;
            private readonly CancellationToken _providedCancellationToken;
            private readonly EventStreamData<TestEventType> _eventOne;
            private IList<EventData>? _submittedEventData;
            private SendEventOptions? _submittedSendEventOptions;
            private CancellationToken _submittedCancellationToken;

            public Without_a_LogAction()
            {
                _eventStreamProducer = GetEventStreamProducer(AutoMocker);
                _providedCancellationToken = new CancellationToken();
                _eventOne = GetTestEvent(_eventIdOne, _eventNameOne,
                    new Dictionary<string, object> { { _propertyKeyOne, _propertyValueOne } });
                SetupMocks();
            }

            private void SetupMocks()
            {
                AutoMocker
                    .GetMock<IEventHubProducerClient>()
                    .Setup(x => x.SendAsync(It.IsAny<IEnumerable<EventData>>(), It.IsAny<SendEventOptions>(),
                        It.IsAny<CancellationToken>()))
                    .Callback<IEnumerable<EventData>, SendEventOptions, CancellationToken>(
                        (eventData, sendEventOptions, ct) =>
                        {
                            _submittedEventData = eventData.ToList();
                            _submittedSendEventOptions = sendEventOptions;
                            _submittedCancellationToken = ct;
                        });
            }

            [Fact]
            public async Task Then_the_event_should_be_published()
            {
                await _eventStreamProducer.SendAsync(
                    _eventOne,
                    _partitionKey,
                    _providedCancellationToken);

                _submittedEventData.Should().HaveCount(1);
            }

            [Fact]
            public async Task Then_the_first_event_published_should_contain_the_correct_data()
            {
                await _eventStreamProducer.SendAsync(
                    _eventOne,
                    _partitionKey,
                    _providedCancellationToken);

                var messageByteArray = JsonSerializer.SerializeToUtf8Bytes(_eventOne.EventData);
                var expectedBinaryData = BinaryData.FromBytes(messageByteArray);

                var firstSubmittedEvent = _submittedEventData!.First();

                firstSubmittedEvent.MessageId.Should().Be(_eventIdOne.ToString());
                firstSubmittedEvent.Properties.Should().ContainKey(_propertyKeyOne);
                firstSubmittedEvent.Properties[_propertyKeyOne].Should().Be(_propertyValueOne);
                firstSubmittedEvent.Data!.ToString().Should().BeEquivalentTo(expectedBinaryData.ToString());
            }

            [Fact]
            public async Task Then_the_partition_key_should_be_set()
            {
                await _eventStreamProducer.SendAsync(
                    _eventOne,
                    _partitionKey,
                    _providedCancellationToken);

                _submittedSendEventOptions!.PartitionKey.Should().Be(_partitionKey);
            }

            [Fact]
            public async Task Then_the_cancellation_token_should_be_passed_through()
            {
                await _eventStreamProducer.SendAsync(
                    _eventOne,
                    _partitionKey,
                    _providedCancellationToken);

                _submittedCancellationToken.Should().Be(_providedCancellationToken);
            }
        }

        public class With_a_LogAction : TestBase
        {
            private readonly EventStreamProducer _eventStreamProducer;
            private readonly List<EventStreamProducerLogItem> _submittedEventStreamProducerLogItems;
            private readonly EventStreamData<TestEventType> _eventOne;
            private readonly DateTime _now;

            public With_a_LogAction()
            {
                _eventStreamProducer = GetEventStreamProducer(AutoMocker);
                _submittedEventStreamProducerLogItems = new List<EventStreamProducerLogItem>();
                _eventOne = GetTestEvent(_eventIdOne, _eventNameOne,
                    new Dictionary<string, object> { { _propertyKeyOne, _propertyValueOne } });
                _now = DateTime.Now;
                SetupMocks();
            }

            private void SetupMocks()
            {
                //AutoMocker
                //    .GetMock<ICorrelationContext>()
                //    .Setup(x => x.GetOrGenerateCurrentCorrelationId())
                //    .Returns(_correlationId);

                //AutoMocker
                //    .GetMock<IServiceResolver>()
                //    .Setup(x => x.Get<ICorrelationContext>())
                //    .Returns(AutoMocker.GetMock<ICorrelationContext>().Object);

                AutoMocker
                    .GetMock<IDateTimeProvider>()
                    .Setup(x => x.Now)
                    .Returns(_now);

                AutoMocker
                    .GetMock<IEventHubProducerClient>()
                    .Setup(x => x.EventHubName)
                    .Returns(_eventHubName);
            }

            private void LogAction(EventStreamProducerLogItem logItem)
            {
                _submittedEventStreamProducerLogItems.Add(logItem);
            }

            [Fact]
            public async Task Then_the_log_action_should_be_called_with_the_correct_data()
            {
                _eventStreamProducer.AddLogging(LogAction);

                await _eventStreamProducer.SendAsync(
                    _eventOne,
                    _partitionKey);

                var eventSerializedOne = JsonSerializer.SerializeToUtf8Bytes(_eventOne.EventData);

                _submittedEventStreamProducerLogItems.Should().BeEquivalentTo(new List<EventStreamProducerLogItem>()
                {
                    new EventStreamProducerLogItem()
                    {
                        EventId = _eventIdOne.ToString(),
                        EventHubName = _eventHubName,
                        PartitionKey = _partitionKey,
                        EventContent = Encoding.UTF8.GetString(eventSerializedOne),
                        Timestamp = _now
                    }
                });
            }
        }

        public class Without_a_partition_key : TestBase
        {
            private readonly EventStreamProducer _eventStreamProducer;
            private readonly CancellationToken _providedCancellationToken;
            private readonly EventStreamData<TestEventType> _eventOne;
            private SendEventOptions? _submittedSendEventOptions;

            public Without_a_partition_key()
            {
                _eventStreamProducer = GetEventStreamProducer(AutoMocker);
                _providedCancellationToken = new CancellationToken();
                _eventOne = GetTestEvent(_eventIdOne, _eventNameOne, new Dictionary<string, object> { { _propertyKeyOne, _propertyValueOne } });
                SetupMocks();
            }

            private void SetupMocks()
            {
                AutoMocker
                    .GetMock<IEventHubProducerClient>()
                    .Setup(x => x.SendAsync(It.IsAny<IEnumerable<EventData>>(), It.IsAny<SendEventOptions>(),
                        It.IsAny<CancellationToken>()))
                    .Callback<IEnumerable<EventData>, SendEventOptions, CancellationToken>(
                        (_, sendEventOptions, _) =>
                        {
                            _submittedSendEventOptions = sendEventOptions;
                        });
            }

            [Fact]
            public async Task Then_the_partition_key_should_not_be_set()
            {
                await _eventStreamProducer.SendAsync(
                    _eventOne,
                    null,
                    _providedCancellationToken);

                _submittedSendEventOptions!.PartitionKey.Should().BeNull();
            }
        }
    }

    public class When_calling_SendAsync_with_multiple_events
    {
        public class Without_a_LogAction : TestBase
        {
            private readonly EventStreamProducer _eventStreamProducer;
            private readonly CancellationToken _providedCancellationToken;
            private readonly EventStreamData<TestEventType> _eventOne;
            private readonly EventStreamData<TestEventType> _eventTwo;
            private IList<EventData>? _submittedEventData;
            private SendEventOptions? _submittedSendEventOptions;
            private CancellationToken _submittedCancellationToken;

            public Without_a_LogAction()
            {
                _eventStreamProducer = GetEventStreamProducer(AutoMocker);
                _providedCancellationToken = new CancellationToken();
                _eventOne = GetTestEvent(_eventIdOne, _eventNameOne,
                    new Dictionary<string, object> { { _propertyKeyOne, _propertyValueOne } });
                _eventTwo = GetTestEvent(_eventIdTwo, _eventNameTwo,
                    new Dictionary<string, object> { { _propertyKeyTwo, _propertyValueTwo } });
                SetupMocks();
            }

            private void SetupMocks()
            {
                AutoMocker
                    .GetMock<IEventHubProducerClient>()
                    .Setup(x => x.SendAsync(It.IsAny<IEnumerable<EventData>>(), It.IsAny<SendEventOptions>(),
                        It.IsAny<CancellationToken>()))
                    .Callback<IEnumerable<EventData>, SendEventOptions, CancellationToken>(
                        (eventData, sendEventOptions, ct) =>
                        {
                            _submittedEventData = eventData.ToList();
                            _submittedSendEventOptions = sendEventOptions;
                            _submittedCancellationToken = ct;
                        });
            }

            private List<EventStreamData<TestEventType>> GetTestEvents()
            {
                return
                [
                    _eventOne,
                    _eventTwo
                ];
            }

            [Fact]
            public async Task Then_the_events_should_be_published()
            {
                await _eventStreamProducer.SendAsync(
                    GetTestEvents(),
                    _partitionKey,
                    _providedCancellationToken);

                _submittedEventData.Should().HaveCount(2);
            }

            [Fact]
            public async Task Then_the_first_event_published_should_contain_the_correct_data()
            {
                await _eventStreamProducer.SendAsync(
                    GetTestEvents(),
                    _partitionKey,
                    _providedCancellationToken);

                var messageByteArray = JsonSerializer.SerializeToUtf8Bytes(_eventOne.EventData);
                var expectedBinaryData = BinaryData.FromBytes(messageByteArray);

                var firstSubmittedEvent = _submittedEventData!.First();

                firstSubmittedEvent.MessageId.Should().Be(_eventIdOne.ToString());
                firstSubmittedEvent.Properties.Should().ContainKey(_propertyKeyOne);
                firstSubmittedEvent.Properties[_propertyKeyOne].Should().Be(_propertyValueOne);
                firstSubmittedEvent.Data!.ToString().Should().BeEquivalentTo(expectedBinaryData.ToString());
            }

            [Fact]
            public async Task Then_the_second_event_published_should_contain_the_correct_data()
            {
                await _eventStreamProducer.SendAsync(
                    GetTestEvents(),
                    _partitionKey,
                    _providedCancellationToken);

                var messageByteArray = JsonSerializer.SerializeToUtf8Bytes(_eventTwo.EventData);
                var expectedBinaryData = BinaryData.FromBytes(messageByteArray);

                var secondSubmittedEvent = _submittedEventData![1];

                secondSubmittedEvent.MessageId.Should().Be(_eventIdTwo.ToString());
                secondSubmittedEvent.Properties.Should().ContainKey(_propertyKeyTwo);
                secondSubmittedEvent.Properties[_propertyKeyTwo].Should().Be(_propertyValueTwo);
                secondSubmittedEvent.Data!.ToString().Should().BeEquivalentTo(expectedBinaryData.ToString());
            }

            [Fact]
            public async Task Then_the_partition_key_should_be_set()
            {
                await _eventStreamProducer.SendAsync(
                    GetTestEvents(),
                    _partitionKey,
                    _providedCancellationToken);

                _submittedSendEventOptions!.PartitionKey.Should().Be(_partitionKey);
            }

            [Fact]
            public async Task Then_the_cancellation_token_should_be_passed_through()
            {
                await _eventStreamProducer.SendAsync(
                    GetTestEvents(),
                    _partitionKey,
                    _providedCancellationToken);

                _submittedCancellationToken.Should().Be(_providedCancellationToken);
            }
        }

        public class With_a_LogAction : TestBase
        {
            private readonly EventStreamProducer _eventStreamProducer;
            private readonly List<EventStreamProducerLogItem> _submittedEventStreamProducerLogItems;
            private readonly EventStreamData<TestEventType> _eventOne;
            private readonly EventStreamData<TestEventType> _eventTwo;
            private readonly DateTime _now;

            public With_a_LogAction()
            {
                _eventStreamProducer = GetEventStreamProducer(AutoMocker);
                _submittedEventStreamProducerLogItems = new List<EventStreamProducerLogItem>();
                _eventOne = GetTestEvent(_eventIdOne, _eventNameOne,
                    new Dictionary<string, object> { { _propertyKeyOne, _propertyValueOne } });
                _eventTwo = GetTestEvent(_eventIdTwo, _eventNameTwo,
                    new Dictionary<string, object> { { _propertyKeyTwo, _propertyValueTwo } });
                _now = DateTime.Now;
                SetupMocks();
            }

            private void SetupMocks()
            {
                AutoMocker
                    .GetMock<IDateTimeProvider>()
                    .Setup(x => x.Now)
                    .Returns(_now);

                AutoMocker
                    .GetMock<IEventHubProducerClient>()
                    .Setup(x => x.EventHubName)
                    .Returns(_eventHubName);
            }

            private List<EventStreamData<TestEventType>> GetTestEvents()
            {
                return
                [
                    _eventOne,
                    _eventTwo
                ];
            }

            private void LogAction(EventStreamProducerLogItem logItem)
            {
                _submittedEventStreamProducerLogItems.Add(logItem);
            }

            [Fact]
            public async Task Then_the_log_action_should_be_called_with_the_correct_data()
            {
                _eventStreamProducer.AddLogging(LogAction);

                await _eventStreamProducer.SendAsync(
                    GetTestEvents(),
                    _partitionKey);

                var eventSerializedOne = JsonSerializer.Serialize(_eventOne.EventData);
                var eventSerializedTwo = JsonSerializer.Serialize(_eventTwo.EventData);

                _submittedEventStreamProducerLogItems.Should().BeEquivalentTo(new List<EventStreamProducerLogItem>()
                {
                    new EventStreamProducerLogItem()
                    {
                        EventId = _eventIdOne.ToString(),
                        EventHubName = _eventHubName,
                        PartitionKey = _partitionKey,
                        EventContent = eventSerializedOne,
                        Timestamp = _now
                    },
                    new EventStreamProducerLogItem()
                    {
                        EventId = _eventIdTwo.ToString(),
                        EventHubName = _eventHubName,
                        PartitionKey = _partitionKey,
                        EventContent = eventSerializedTwo,
                        Timestamp = _now
                    },
                });
            }
        }

        public class Without_a_partition_key : TestBase
        {
            private readonly EventStreamProducer _eventStreamProducer;
            private readonly CancellationToken _providedCancellationToken;
            private readonly EventStreamData<TestEventType> _eventOne;
            private readonly EventStreamData<TestEventType> _eventTwo;
            private SendEventOptions? _submittedSendEventOptions;
            
            public Without_a_partition_key()
            {
                _eventStreamProducer = GetEventStreamProducer(AutoMocker);
                _providedCancellationToken = new CancellationToken();
                _eventOne = GetTestEvent(_eventIdOne, _eventNameOne,
                    new Dictionary<string, object> { { _propertyKeyOne, _propertyValueOne } });
                _eventTwo = GetTestEvent(_eventIdTwo, _eventNameTwo,
                    new Dictionary<string, object> { { _propertyKeyTwo, _propertyValueTwo } });
                SetupMocks();
            }

            private void SetupMocks()
            {
                AutoMocker
                    .GetMock<IEventHubProducerClient>()
                    .Setup(x => x.SendAsync(It.IsAny<IEnumerable<EventData>>(), It.IsAny<SendEventOptions>(),
                        It.IsAny<CancellationToken>()))
                    .Callback<IEnumerable<EventData>, SendEventOptions, CancellationToken>(
                        (_, sendEventOptions, _) =>
                        {
                            _submittedSendEventOptions = sendEventOptions;
                        });
            }

            private List<EventStreamData<TestEventType>> GetTestEvents()
            {
                return
                [
                    _eventOne,
                    _eventTwo
                ];
            }

            [Fact]
            public async Task Then_the_partition_key_should_not_be_set()
            {
                await _eventStreamProducer.SendAsync(
                    GetTestEvents(),
                    null,
                    _providedCancellationToken);

                _submittedSendEventOptions!.PartitionKey.Should().BeNull();
            }
        }
    }
    
    public class When_calling_CloseAsync
    {
        public class And_client_is_not_null : TestBase
        {
            private readonly EventStreamProducer _eventStreamProducer;

            public And_client_is_not_null()
            {
                _eventStreamProducer = GetEventStreamProducer(AutoMocker);
            }

            [Fact]
            public async Task Then_it_should_close_the_client()
            {
                await _eventStreamProducer.CloseAsync();
                AutoMocker.GetMock<IEventHubProducerClient>()
                    .Verify(x => x.CloseAsync(It.IsAny<CancellationToken>()), Times.Once);
            }
        }
    }

    public class When_calling_AddLogging : TestBase
    {
        private readonly EventStreamProducer _eventStreamProducer;
        private readonly List<EventStreamProducerLogItem> _submittedEventStreamProducerLogItems;
        private readonly EventStreamData<TestEventType> _eventOne;
        private readonly DateTime _now;

        public When_calling_AddLogging()
        {
            _eventStreamProducer = GetEventStreamProducer(AutoMocker);
            _submittedEventStreamProducerLogItems = new List<EventStreamProducerLogItem>();
            _eventOne = GetTestEvent(
                _eventIdOne,
                _eventNameOne,
                new Dictionary<string, object> { { _propertyKeyOne, _propertyValueOne } });
            _now = DateTime.Now;
            SetupMocks();
        }

        private void SetupMocks()
        {
            AutoMocker
                .GetMock<IDateTimeProvider>()
                .Setup(x => x.Now)
                .Returns(_now);

            AutoMocker
                .GetMock<IEventHubProducerClient>()
                .Setup(x => x.EventHubName)
                .Returns(_eventHubName);
        }

        private void LogAction(EventStreamProducerLogItem logItem)
        {
            _submittedEventStreamProducerLogItems.Add(logItem);
        }

        [Fact]
        public void Then_the_log_action_should_be_called_with_the_correct_data()
        {
            _eventStreamProducer.AddLogging(LogAction);

            _eventStreamProducer.SendAsync(_eventOne, _partitionKey);

            var eventSerializedOne = JsonSerializer.Serialize(_eventOne.EventData);

            _submittedEventStreamProducerLogItems.Should().BeEquivalentTo(new List<EventStreamProducerLogItem>()
            {
                new EventStreamProducerLogItem()
                {
                    EventId = _eventIdOne.ToString(),
                    EventHubName = _eventHubName,
                    PartitionKey = _partitionKey,
                    EventContent = eventSerializedOne,
                    Timestamp = _now
                }
            });
        }
    }
}
