using Microsoft.Extensions.DependencyInjection;

namespace OutboxCore.Configuration;

public class OutboxModuleBuilder
{
    private readonly OutboxBuilder _parentBuilder;

    public IServiceCollection Services { get; }
    public string ModuleName { get; }

    public OutboxModuleBuilder(OutboxBuilder parentBuilder, string moduleName)
    {
        _parentBuilder = parentBuilder;
        Services = parentBuilder.Services;
        ModuleName = moduleName;
    }

    public OutboxBuilder And() => _parentBuilder;
}
