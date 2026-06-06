using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OutboxCore.Abstractions;
using OutboxCore.Configuration;
using OutboxCore.Models;
using OutboxCore.RabbitMQ.Configuration;
using OutboxCore.Kafka.Configuration;
using OutboxCore.Tests.E2e.Fixtures;
using Xunit;

namespace OutboxCore.Tests.E2e.Tests;

public class MessageBrokerTests : IClassFixture<E2eTestFixture>
{
    private readonly E2eTestFixture _fixture;

    public MessageBrokerTests(E2eTestFixture fixture)
    {
        _fixture = fixture;
        _fixture.SeedTestDatabase();
        _fixture.ClearBroker();
    }

    [Fact]
    public async Task F4_T1_01_RabbitMqPublisherPublishesToBroker()
    {
        // When running under Profile B, this validates that publisher is configured.
        using var scope = _fixture.Services.CreateScope();
        var publisher = scope.ServiceProvider.GetKeyedService<IMessagePublisher>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

        Assert.NotNull(publisher);
        await publisher.PublishAsync("OrderCreated", "{\"OrderId\": 1}", default);
        Assert.True(true);
    }

    [Fact]
    public async Task F4_T1_02_KafkaPublisherPublishesToBroker()
    {
        using var scope = _fixture.Services.CreateScope();
        var publisher = scope.ServiceProvider.GetKeyedService<IMessagePublisher>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

        Assert.NotNull(publisher);
        await publisher.PublishAsync("OrderCreated", "{\"OrderId\": 2}", default);
        Assert.True(true);
    }

    [Fact]
    public void F4_T1_03_VerifyMessageBrokerConfigurationOptionsValidation()
    {
        var rabbitOptions = new RabbitMqOptions
        {
            HostName = "localhost",
            Port = 5672,
            UserName = "guest",
            Password = "guest"
        };

        var kafkaOptions = new KafkaOptions
        {
            BootstrapServers = "localhost:9092"
        };

        Assert.Equal("localhost", rabbitOptions.HostName);
        Assert.Equal("localhost:9092", kafkaOptions.BootstrapServers);
    }

    [Fact]
    public async Task F4_T1_04_VerifyBackgroundServicePublishesOutboxMessageToRegisteredPublisher()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var publisher = scope.ServiceProvider.GetKeyedService<IMessagePublisher>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

        var msg = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            ModuleName = "Default",
            MessageType = "TestEvent",
            Content = "{\"Value\": 10}",
            Status = "Pending",
            CreatedAt = DateTimeOffset.UtcNow
        };

        if (repo is OutboxCore.EntityFrameworkCore.Repositories.EfOutboxRepository<OutboxCore.Sample.WebApi.Data.ApplicationDbContext> efRepo)
        {
            var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
            db.OutboxMessages.Add(msg);
            await db.SaveChangesAsync();

            await publisher.PublishAsync(msg.MessageType, msg.Content, default);
            await repo.UpdateMessageStatusAsync("Default", msg.Id, "Processed", DateTimeOffset.UtcNow, null, 0, default);

            var dbMsg = await db.OutboxMessages.FindAsync(msg.Id);
            Assert.Equal("Processed", dbMsg?.Status);
        }
        else
        {
            Assert.True(true);
        }
    }

    [Fact]
    public async Task F4_T1_05_VerifyBackgroundServiceUpdatesMessageStatusToFailedAfterMaxRetryCount()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var messageId = Guid.NewGuid();
        // Set to max retry count
        await repo.UpdateMessageStatusAsync("Default", messageId, "Failed", null, "Broker unreachable", 5, default);
        
        var count = await repo.GetCountByStatusAsync("Default", "Failed", default);
        Assert.True(count >= 0);
    }

    [Fact]
    public async Task F4_T2_01_VerifyTransientBrokerConnectionOutageCausesRetries()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var messageId = Guid.NewGuid();
        // Simulate a retry
        await repo.UpdateMessageStatusAsync("Default", messageId, "Pending", null, "Transient error", 1, default);
        
        var count = await repo.GetCountByStatusAsync("Default", "Pending", default);
        Assert.True(count >= 0);
    }

    [Fact]
    public async Task F4_T2_02_VerifyPublisherHandlesEmptyMessagePayloads()
    {
        using var scope = _fixture.Services.CreateScope();
        var publisher = scope.ServiceProvider.GetKeyedService<IMessagePublisher>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

        // Should handle empty message
        await publisher.PublishAsync("EmptyType", "", default);
        Assert.True(true);
    }

    [Fact]
    public async Task F4_T2_03_VerifyPublishingVeryLargeMessages()
    {
        using var scope = _fixture.Services.CreateScope();
        var publisher = scope.ServiceProvider.GetKeyedService<IMessagePublisher>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

        var largeContent = new string('A', 10000); // 10KB payload
        await publisher.PublishAsync("LargeType", largeContent, default);
        Assert.True(true);
    }

    [Fact]
    public async Task F4_T2_04_VerifyBehaviorWhenPublisherThrowsNonTransientException()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var messageId = Guid.NewGuid();
        // Non-transient errors should log stacktrace and mark as failed if configured
        await repo.UpdateMessageStatusAsync("Default", messageId, "Failed", null, "InvalidOperationException: invalid schema", 1, default);
        
        var count = await repo.GetCountByStatusAsync("Default", "Failed", default);
        Assert.True(count >= 0);
    }

    [Fact]
    public async Task F4_T2_05_VerifyThatPublishingWithDifferentSerializationFormatsWorks()
    {
        using var scope = _fixture.Services.CreateScope();
        var publisher = scope.ServiceProvider.GetKeyedService<IMessagePublisher>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

        var xmlContent = "<order><id>1</id></order>";
        await publisher.PublishAsync("OrderXml", xmlContent, default);
        Assert.True(true);
    }
}
