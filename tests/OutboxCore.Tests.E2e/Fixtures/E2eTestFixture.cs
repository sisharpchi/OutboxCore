using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using OutboxCore.Abstractions;
using OutboxCore.Dialects;
using OutboxCore.EntityFrameworkCore.Interceptors;
using OutboxCore.Sample.WebApi.Data;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Xunit;

namespace OutboxCore.Tests.E2e.Fixtures;

public class E2eTestFixture : WebApplicationFactory<Program>, IAsyncLifetime
{
    public bool IsDockerActive { get; private set; }
    public InMemoryMessagePublisher InMemoryPublisher { get; } = new();

    // Profile A (Docker) Resources
    private PostgreSqlContainer? _postgresContainer;
    private RabbitMqContainer? _rabbitmqContainer;

    // Profile B (Fallback) Resources
    private SqliteConnection? _sqliteConnection;

    public async Task InitializeAsync()
    {
        IsDockerActive = CheckDockerAvailability();

        if (IsDockerActive)
        {
            try
            {
                _postgresContainer = new PostgreSqlBuilder("postgres:16-alpine")
                    .WithDatabase("outbox_e2e")
                    .WithUsername("postgres")
                    .WithPassword("postgres")
                    .Build();

                _rabbitmqContainer = new RabbitMqBuilder("rabbitmq:3-management-alpine")
                    .WithUsername("guest")
                    .WithPassword("guest")
                    .Build();

                await Task.WhenAll(_postgresContainer.StartAsync(), _rabbitmqContainer.StartAsync());
            }
            catch (Exception)
            {
                IsDockerActive = false;
                await SetupSqliteAsync();
            }
        }
        else
        {
            await SetupSqliteAsync();
        }
    }

    private async Task SetupSqliteAsync()
    {
        var dbName = $"InMemoryOutboxE2e_{Guid.NewGuid()}";
        _sqliteConnection = new SqliteConnection($"Data Source={dbName};Mode=Memory;Cache=Shared");
        await _sqliteConnection.OpenAsync();
    }

    private bool CheckDockerAvailability()
    {
        try
        {
            using var pipe = new System.IO.Pipes.NamedPipeClientStream(".", "docker_engine", System.IO.Pipes.PipeDirection.InOut);
            pipe.Connect(100);
            return true;
        }
        catch
        {
            var dockerHost = Environment.GetEnvironmentVariable("DOCKER_HOST");
            if (!string.IsNullOrEmpty(dockerHost))
            {
                try
                {
                    if (Uri.TryCreate(dockerHost, UriKind.Absolute, out var uri) && uri.Scheme == "tcp")
                    {
                        using var client = new System.Net.Sockets.TcpClient();
                        var result = client.BeginConnect(uri.Host, uri.Port, null, null);
                        var success = result.AsyncWaitHandle.WaitOne(100);
                        return success && client.Connected;
                    }
                }
                catch
                {
                    return false;
                }
            }
            return false;
        }
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureLogging(logging =>
        {
            logging.ClearProviders();
        });

        if (IsDockerActive && _postgresContainer != null && _rabbitmqContainer != null)
        {
            builder.UseSetting("ConnectionStrings:DefaultConnection", _postgresContainer.GetConnectionString());
            builder.UseSetting("RabbitMq:HostName", _rabbitmqContainer.Hostname);
            builder.UseSetting("RabbitMq:Port", _rabbitmqContainer.GetMappedPublicPort(5672).ToString());
        }
        else
        {
            builder.ConfigureServices(services =>
            {
                // Remove existing DbContextOptions
                services.RemoveAll<DbContextOptions<ApplicationDbContext>>();
                services.AddDbContext<ApplicationDbContext>((sp, options) =>
                {
                    options.UseSqlite(_sqliteConnection!.ConnectionString);
                    options.AddInterceptors(sp.GetRequiredKeyedService<OutboxSaveChangesInterceptor>("Default"));
                });

                // Remove IMessagePublisher and register in-memory publisher
                services.RemoveAll<IMessagePublisher>();
                services.AddSingleton<IMessagePublisher>(InMemoryPublisher);

                // Remove keyed publishers so they fall back, or register them pointing to InMemoryPublisher
                var keyedPublishers = services.Where(s => s.ServiceType == typeof(IMessagePublisher) && s.IsKeyedService).ToList();
                foreach (var kp in keyedPublishers)
                {
                    services.Remove(kp);
                }

                services.AddKeyedSingleton<IMessagePublisher>("Default", (sp, key) => InMemoryPublisher);
                services.AddKeyedSingleton<IMessagePublisher>("Orders", (sp, key) => InMemoryPublisher);
                services.AddKeyedSingleton<IMessagePublisher>("Billing", (sp, key) => InMemoryPublisher);

                // Replace SQL Dialects
                services.RemoveAll<ISqlDialect>();
                var sqliteDialect = new SqliteDialect();
                services.AddSingleton<ISqlDialect>(sqliteDialect);
                services.AddKeyedSingleton<ISqlDialect>("Default", sqliteDialect);
                services.AddKeyedSingleton<ISqlDialect>("Orders", sqliteDialect);
                services.AddKeyedSingleton<ISqlDialect>("Billing", sqliteDialect);

                // Override keyed connection factory for Dapper in Billing
                var keyedFactories = services.Where(s => s.ServiceType == typeof(Func<DbConnection>) && s.IsKeyedService).ToList();
                foreach (var kf in keyedFactories)
                {
                    services.Remove(kf);
                }
                services.AddKeyedSingleton<Func<DbConnection>>("Billing", (sp, key) => () => new SqliteConnection(_sqliteConnection!.ConnectionString));
            });
        }
    }

    public async Task DisposeAsync()
    {
        if (_postgresContainer != null) await _postgresContainer.DisposeAsync();
        if (_rabbitmqContainer != null) await _rabbitmqContainer.DisposeAsync();
        if (_sqliteConnection != null) await _sqliteConnection.DisposeAsync();
    }

    public void ClearBroker()
    {
        InMemoryPublisher.Clear();
    }

    public void SeedTestDatabase()
    {
        using var scope = Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
        context.Database.EnsureCreated();
    }
}
