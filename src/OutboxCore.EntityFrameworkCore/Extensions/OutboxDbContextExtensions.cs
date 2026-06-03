using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OutboxCore.Abstractions;
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
        builder.Services.TryAddSingleton<OutboxSaveChangesInterceptor>();

        return builder;
    }
}
