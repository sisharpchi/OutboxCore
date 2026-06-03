using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Moq;
using OutboxCore.Background;
using OutboxCore.EntityFrameworkCore.Interceptors;
using OutboxCore.EntityFrameworkCore.Extensions;
using OutboxCore.Models;
using OutboxCore.Abstractions;
using Xunit;

namespace OutboxCore.Tests.Unit;

public class TestEvent : IDomainEvent
{
    public Guid Id { get; } = Guid.NewGuid();
    public DateTimeOffset OccurredOn { get; } = DateTimeOffset.UtcNow;
    public string Data { get; set; } = "Test";
}

public class TestEntity : AggregateRoot
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;

    public void DoSomething()
    {
        RaiseDomainEvent(new TestEvent { Data = Name });
    }
}

public class TestDbContext : DbContext
{
    public DbSet<TestEntity> TestEntities { get; set; } = null!;
    public DbSet<OutboxMessage> OutboxMessages { get; set; } = null!;
    public DbSet<InboxMessage> InboxMessages { get; set; } = null!;

    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.ApplyOutboxConfigurations();

        modelBuilder.Entity<TestEntity>(builder =>
        {
            builder.HasKey(x => x.Id);
        });
    }
}

public class OutboxSaveChangesInterceptorTests : IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly DbContextOptions<TestDbContext> _options;
    private readonly Mock<IOutboxChannel> _channelMock;
    private readonly OutboxSaveChangesInterceptor _interceptor;

    public OutboxSaveChangesInterceptorTests()
    {
        _connection = new SqliteConnection("Filename=:memory:");
        _connection.Open();

        _channelMock = new Mock<IOutboxChannel>();
        _interceptor = new OutboxSaveChangesInterceptor(_channelMock.Object);

        _options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite(_connection)
            .AddInterceptors(_interceptor)
            .Options;

        using var context = new TestDbContext(_options);
        context.Database.EnsureCreated();
    }

    [Fact]
    public async Task SaveChangesAsync_ShouldConvertDomainEventsToOutboxMessagesAndTriggerChannel()
    {
        // Arrange
        using var context = new TestDbContext(_options);
        var entity = new TestEntity { Id = Guid.NewGuid(), Name = "Entity1" };
        entity.DoSomething();
        context.TestEntities.Add(entity);

        // Act
        await context.SaveChangesAsync();

        // Assert
        using var checkContext = new TestDbContext(_options);
        var outboxMessages = await checkContext.OutboxMessages.ToListAsync();

        Assert.Single(outboxMessages);
        var message = outboxMessages.First();
        Assert.Equal("OutboxCore.Tests.Unit.TestEvent", message.MessageType);
        Assert.Contains("Entity1", message.Content);
        Assert.Equal("Pending", message.Status);
        Assert.Equal(0, message.RetryCount);

        // Ensure DomainEvents are cleared on the original entity
        Assert.Empty(entity.DomainEvents);

        // Verify channel notification was triggered
        _channelMock.Verify(x => x.WriteAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    public void Dispose()
    {
        _connection.Dispose();
    }
}
