using Microsoft.Extensions.DependencyInjection;
using Ninject.MockingKernel.Moq;

namespace Foundation.EventStreaming.EventHubs.Tests;

public class TestBase
{
    protected MoqMockingKernel AutoMocker { get; } = GetMoqMockingKernel();

    private static MoqMockingKernel GetMoqMockingKernel()
    {
        return new MoqMockingKernel();
    }

    protected void SetupServiceScope()
    {
        var mockScope = AutoMocker.GetMock<IServiceScope>();
        var mockScopeFactory = AutoMocker.GetMock<IServiceScopeFactory>();
        AutoMocker.GetMock<IServiceProvider>().Setup(x => x.GetService(typeof(IServiceScopeFactory))).Returns(mockScopeFactory.Object);
        mockScopeFactory.Setup(x => x.CreateScope()).Returns(mockScope.Object);
    }
}