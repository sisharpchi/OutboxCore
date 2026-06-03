using Microsoft.Extensions.DependencyInjection;

namespace OutboxCore.Configuration;

public class OutboxBuilder
{
    public IServiceCollection Services { get; }

    public OutboxBuilder(IServiceCollection services)
    {
        Services = services;
    }
}
