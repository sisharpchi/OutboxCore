using System;
using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OutboxCore.Abstractions;
using OutboxCore.Configuration;
using OutboxCore.Dapper.Repositories;

namespace OutboxCore.Dapper.Extensions;

public static class OutboxDapperExtensions
{
    public static OutboxBuilder UseDapper(
        this OutboxBuilder builder,
        Func<IServiceProvider, DbConnection> connectionFactory,
        ISqlDialect dialect)
    {
        builder.Services.TryAddSingleton(dialect);
        builder.Services.TryAddSingleton<Func<DbConnection>>(sp => () => connectionFactory(sp));
        builder.Services.TryAddScoped<IOutboxRepository>(sp =>
        {
            var factory = sp.GetRequiredService<Func<DbConnection>>();
            var d = sp.GetRequiredService<ISqlDialect>();
            return new DapperOutboxRepository(factory, d);
        });
        builder.Services.TryAddScoped<IInboxProcessor>(sp =>
        {
            var factory = sp.GetRequiredService<Func<DbConnection>>();
            return new DapperInboxProcessor(factory);
        });

        return builder;
    }

    public static OutboxModuleBuilder UseDapper(
        this OutboxModuleBuilder builder,
        Func<IServiceProvider, DbConnection> connectionFactory,
        ISqlDialect dialect)
    {
        builder.Services.TryAddKeyedSingleton<ISqlDialect>(builder.ModuleName, dialect);
        builder.Services.TryAddKeyedSingleton<Func<DbConnection>>(builder.ModuleName, (sp, key) => () => connectionFactory(sp));

        builder.Services.TryAddKeyedScoped<IOutboxRepository>(builder.ModuleName, (sp, key) =>
        {
            var factory = sp.GetRequiredKeyedService<Func<DbConnection>>(key);
            var d = sp.GetRequiredKeyedService<ISqlDialect>(key);
            return new DapperOutboxRepository(factory, d);
        });

        builder.Services.TryAddKeyedScoped<IInboxProcessor>(builder.ModuleName, (sp, key) =>
        {
            var factory = sp.GetRequiredKeyedService<Func<DbConnection>>(key);
            return new DapperInboxProcessor(factory);
        });

        return builder;
    }
}
