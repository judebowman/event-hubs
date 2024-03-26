using Azure.Core;
using FluentAssertions;
using Foundation.EventStreaming.EventHubs.Exceptions;
using Foundation.EventStreaming.EventHubs.Producer;
using Moq;

namespace Foundation.EventStreaming.EventHubs.Tests.Producer;

public class EventStreamProducerSettingsTests
{
    private static EventStreamProducerSettings GetValidSettings()
    {
        return new EventStreamProducerSettings
        {
            AzureEventHubsFullyQualifiedNamespace = "namespace",
            AzureTokenCredential = new Mock<TokenCredential>().Object,
            AzureEventHubsName = "event hubs name"
        };
    }

    public class When_calling_Validate
    {
        public class With_valid_settings
        {
            [Fact]
            public void It_should_not_throw()
            {
                var settings = GetValidSettings();
                var exception = Record.Exception(() => settings.Validate());
                exception.Should().BeNull();
            }
        }

        public class With_a_null_AzureEventHubsFullyQualifiedNamespace
        {
            [Fact]
            public void It_should_throw()
            {
                var settings = GetValidSettings();
                settings.AzureEventHubsFullyQualifiedNamespace = null;
                var exception = Record.Exception(() => settings.Validate());
                exception.Should().NotBeNull();
                exception.Should().BeOfType<EventStreamSetupException>();
                exception!.Message.Should().Be("AzureEventHubsFullyQualifiedNamespace is required.");
            }
        }

        public class With_an_empty_AzureEventHubsFullyQualifiedNamespace
        {
            [Fact]
            public void It_should_throw()
            {
                var settings = GetValidSettings();
                settings.AzureEventHubsFullyQualifiedNamespace = string.Empty;
                var exception = Record.Exception(() => settings.Validate());
                exception.Should().NotBeNull();
                exception.Should().BeOfType<EventStreamSetupException>();
                exception!.Message.Should().Be("AzureEventHubsFullyQualifiedNamespace is required.");
            }
        }

        public class With_a_null_AzureTokenCredential
        {
            [Fact]
            public void It_should_throw()
            {
                var settings = GetValidSettings();
                settings.AzureTokenCredential = null;
                var exception = Record.Exception(() => settings.Validate());
                exception.Should().NotBeNull();
                exception.Should().BeOfType<EventStreamSetupException>();
                exception!.Message.Should().Be("AzureTokenCredential is required.");
            }
        }

        public class With_a_null_AzureEventHubsName
        {
            [Fact]
            public void It_should_throw()
            {
                var settings = GetValidSettings();
                settings.AzureEventHubsName = null;
                var exception = Record.Exception(() => settings.Validate());
                exception.Should().NotBeNull();
                exception.Should().BeOfType<EventStreamSetupException>();
                exception!.Message.Should().Be("AzureEventHubsName is required.");
            }
        }

        public class With_an_empty_AzureEventHubsName
        {
            [Fact]
            public void It_should_throw()
            {
                var settings = GetValidSettings();
                settings.AzureEventHubsName = string.Empty;
                var exception = Record.Exception(() => settings.Validate());
                exception.Should().NotBeNull();
                exception.Should().BeOfType<EventStreamSetupException>();
                exception!.Message.Should().Be("AzureEventHubsName is required.");
            }
        }
    }
}