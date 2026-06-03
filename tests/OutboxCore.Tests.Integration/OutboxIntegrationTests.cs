using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OutboxCore.Sample.WebApi.Data;
using OutboxCore.Sample.WebApi.Controllers;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Xunit;

namespace OutboxCore.Tests.Integration;

public class OutboxIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgresContainer = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("outbox_test")
        .WithUsername("postgres")
        .WithPassword("postgres")
        .Build();

    private readonly RabbitMqContainer _rabbitmqContainer = new RabbitMqBuilder("rabbitmq:3-management-alpine")
        .WithUsername("guest")
        .WithPassword("guest")
        .Build();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgresContainer.StartAsync(), _rabbitmqContainer.StartAsync());
    }

    public async Task DisposeAsync()
    {
        await Task.WhenAll(_postgresContainer.DisposeAsync().AsTask(), _rabbitmqContainer.DisposeAsync().AsTask());
    }

    [Fact]
    public async Task PostOrder_ShouldSaveToDb_AndPublishToBroker_AndMarkOutboxAsProcessed()
    {
        // Arrange
        var connectionString = _postgresContainer.GetConnectionString();
        var rabbitHost = _rabbitmqContainer.Hostname;
        var rabbitPort = _rabbitmqContainer.GetMappedPublicPort(5672);

        var factory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.UseSetting("ConnectionStrings:DefaultConnection", connectionString);
                builder.UseSetting("RabbitMq:HostName", rabbitHost);
                builder.UseSetting("RabbitMq:Port", rabbitPort.ToString());
            });

        var client = factory.CreateClient();

        // Act - Send request to create order
        var request = new OrdersController.CreateOrderRequest("John Doe", 150.00m);
        var response = await client.PostAsJsonAsync("/api/orders", request);

        // Assert HTTP Success
        response.EnsureSuccessStatusCode();
        var order = await response.Content.ReadFromJsonAsync<OrderResponse>();
        Assert.NotNull(order);
        Assert.Equal("John Doe", order.CustomerName);
        Assert.Equal(150.00m, order.TotalAmount);

        // Wait a few seconds for background worker to process outbox message
        await Task.Delay(4000);

        // Assert database states
        using var scope = factory.Services.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

        var savedOrder = await dbContext.Orders.FindAsync(order.Id);
        Assert.NotNull(savedOrder);

        var outboxMessage = await dbContext.OutboxMessages.FirstOrDefaultAsync();
        Assert.NotNull(outboxMessage);
        Assert.Equal("Processed", outboxMessage.Status);
        Assert.NotNull(outboxMessage.ProcessedAt);
        Assert.Equal(0, outboxMessage.RetryCount);
    }

    private record OrderResponse(Guid Id, string CustomerName, decimal TotalAmount);
}
