using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OutboxCore.Abstractions;
using OutboxCore.Background;
using OutboxCore.Configuration;
using OutboxCore.EntityFrameworkCore.Interceptors;
using OutboxCore.EntityFrameworkCore.Repositories;

namespace OutboxCore.EntityFrameworkCore.Extensions;

public static class OutboxDbContextExtensions
{
    public static ModelBuilder ApplyOutboxConfigurations(this ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new Configurations.OutboxMessageConfiguration());
        modelBuilder.ApplyConfiguration(new Configurations.InboxMessageConfiguration());
        return modelBuilder;
    }

    public static OutboxBuilder UseEntityFrameworkCore<TContext>(
        this OutboxBuilder builder,
        ISqlDialect dialect) where TContext : DbContext
    {
        builder.Services.TryAddSingleton(dialect);
        builder.Services.TryAddScoped<IOutboxRepository, EfOutboxRepository<TContext>>();
        builder.Services.TryAddScoped<IInboxProcessor, EfInboxProcessor<TContext>>();
        builder.Services.TryAddScoped<OutboxSaveChangesInterceptor>();

        return builder;
    }

    public static OutboxModuleBuilder UseEntityFrameworkCore<TContext>(
        this OutboxModuleBuilder builder,
        ISqlDialect dialect) where TContext : DbContext
    {
        // Register keyed SQL dialect for current module
        builder.Services.TryAddKeyedSingleton<ISqlDialect>(builder.ModuleName, dialect);

        // Register repositories keyed by module name
        builder.Services.TryAddKeyedScoped<IOutboxRepository>(builder.ModuleName, (sp, key) =>
        {
            var context = sp.GetRequiredService<TContext>();
            var d = sp.GetRequiredKeyedService<ISqlDialect>(key);
            return new EfOutboxRepository<TContext>(context, d);
        });
        builder.Services.TryAddKeyedScoped<IInboxProcessor, EfInboxProcessor<TContext>>(builder.ModuleName);

        // Register scoped interceptor keyed by module name
        builder.Services.TryAddKeyedScoped<OutboxSaveChangesInterceptor>(builder.ModuleName, (sp, key) =>
        {
            var channel = sp.GetRequiredService<IOutboxChannel>();
            return new OutboxSaveChangesInterceptor(channel, (string)key!);
        });

        return builder;
    }
}
