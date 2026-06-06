using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using OutboxCore.Abstractions;
using OutboxCore.Background;
using OutboxCore.Configuration;
using OutboxCore.Dialects;
using OutboxCore.EntityFrameworkCore.Extensions;
using OutboxCore.EntityFrameworkCore.Interceptors;
using OutboxCore.Models;
using Xunit;

namespace OutboxCore.Tests.Unit;

public class VerificationEvent : IDomainEvent
{
    public Guid Id { get; } = Guid.NewGuid();
    public DateTimeOffset OccurredOn { get; } = DateTimeOffset.UtcNow;
    public string Data { get; set; } = "Verification";
}

public class VerificationEntity : AggregateRoot
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;

    public void DoSomething()
    {
        RaiseDomainEvent(new VerificationEvent { Data = Name });
    }
}

public class VerificationDbContext : DbContext
{
    public DbSet<VerificationEntity> VerificationEntities { get; set; } = null!;
    public DbSet<OutboxMessage> OutboxMessages { get; set; } = null!;
    public DbSet<InboxMessage> InboxMessages { get; set; } = null!;

    public VerificationDbContext(DbContextOptions<VerificationDbContext> options) : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.ApplyOutboxConfigurations();

        modelBuilder.Entity<VerificationEntity>(builder =>
        {
            builder.HasKey(x => x.Id);
        });
    }
}

public class Milestone1VerificationTests
{
    // Verification 1: Parallel Module Isolation & No Cross-Pollution
    [Fact]
    public async Task RunBackgroundService_WithMultipleModules_ShouldProcessIndependentlyWithoutPollution()
    {
        // Arrange
        var services = new ServiceCollection();
        
        var options = new OutboxOptions();
        options.RegisterModule("ModuleA", opts => {
            opts.BatchSize = 10;
            opts.PollingInterval = TimeSpan.FromMilliseconds(50);
            opts.LockDuration = TimeSpan.FromSeconds(5);
        });
        options.RegisterModule("ModuleB", opts => {
            opts.BatchSize = 10;
            opts.PollingInterval = TimeSpan.FromMilliseconds(50);
            opts.LockDuration = TimeSpan.FromSeconds(5);
        });

        var optionsMock = new Mock<IOptions<OutboxOptions>>();
        optionsMock.Setup(o => o.Value).Returns(options);

        var channelMock = new Mock<IOutboxChannel>();
        channelMock.Setup(c => c.WaitToReadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(async (string _, CancellationToken ct) =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                }
                return false;
            });

        // Set up separate repositories & publishers for ModuleA and ModuleB
        var repoAMock = new Mock<IOutboxRepository>();
        var repoBMock = new Mock<IOutboxRepository>();
        var pubAMock = new Mock<IMessagePublisher>();
        var pubBMock = new Mock<IMessagePublisher>();

        var msgA = new OutboxMessage { Id = Guid.NewGuid(), ModuleName = "ModuleA", MessageType = "EventA", Content = "ContentA", Status = "Pending" };
        var msgB = new OutboxMessage { Id = Guid.NewGuid(), ModuleName = "ModuleB", MessageType = "EventB", Content = "ContentB", Status = "Pending" };

        // We use queues to return the locked message only on the first call, and empty lists on subsequent calls.
        // This avoids infinite tight looping and starvation.
        var msgAQueue = new Queue<List<OutboxMessage>>();
        msgAQueue.Enqueue(new List<OutboxMessage> { msgA });
        msgAQueue.Enqueue(new List<OutboxMessage>());

        var msgBQueue = new Queue<List<OutboxMessage>>();
        msgBQueue.Enqueue(new List<OutboxMessage> { msgB });
        msgBQueue.Enqueue(new List<OutboxMessage>());

        repoAMock.Setup(r => r.LockMessagesAsync("ModuleA", It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => msgAQueue.Count > 0 ? msgAQueue.Dequeue() : new List<OutboxMessage>());
        repoBMock.Setup(r => r.LockMessagesAsync("ModuleB", It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => msgBQueue.Count > 0 ? msgBQueue.Dequeue() : new List<OutboxMessage>());

        // Synchronize using TaskCompletionSources for deterministic execution
        var pubAExecuted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pubBExecuted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        pubAMock.Setup(p => p.PublishAsync("EventA", "ContentA", It.IsAny<CancellationToken>()))
            .Callback(() => pubAExecuted.TrySetResult(true))
            .Returns(Task.CompletedTask);

        pubBMock.Setup(p => p.PublishAsync("EventB", "ContentB", It.IsAny<CancellationToken>()))
            .Callback(() => pubBExecuted.TrySetResult(true))
            .Returns(Task.CompletedTask);

        services.AddKeyedSingleton<IOutboxRepository>("ModuleA", repoAMock.Object);
        services.AddKeyedSingleton<IOutboxRepository>("ModuleB", repoBMock.Object);
        services.AddKeyedSingleton<IMessagePublisher>("ModuleA", pubAMock.Object);
        services.AddKeyedSingleton<IMessagePublisher>("ModuleB", pubBMock.Object);

        var serviceProvider = services.BuildServiceProvider();
        var loggerMock = new Mock<ILogger<OutboxPublisherBackgroundService>>();

        var backgroundService = new OutboxPublisherBackgroundService(serviceProvider, channelMock.Object, optionsMock.Object, loggerMock.Object);

        // Act
        using var cts = new CancellationTokenSource();
        var runTask = backgroundService.StartAsync(cts.Token);

        // Wait for both modules to process at least once with a timeout of 5 seconds
        var delayTask = Task.Delay(5000);
        var completedTask = await Task.WhenAny(Task.WhenAll(pubAExecuted.Task, pubBExecuted.Task), delayTask);

        cts.Cancel();
        try { await runTask; } catch (OperationCanceledException) { }

        if (completedTask == delayTask)
        {
            throw new Exception("Timeout waiting for background service tasks to execute.");
        }

        // Assert
        // Verify ModuleA publisher only received EventA, never EventB
        pubAMock.Verify(p => p.PublishAsync("EventA", "ContentA", It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        pubAMock.Verify(p => p.PublishAsync("EventB", It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);

        // Verify ModuleB publisher only received EventB, never EventA
        pubBMock.Verify(p => p.PublishAsync("EventB", "ContentB", It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        pubBMock.Verify(p => p.PublishAsync("EventA", It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);

        // Verify repository updates are isolated to their respective modules
        repoAMock.Verify(r => r.UpdateMessageStatusAsync("ModuleA", msgA.Id, "Processed", It.IsAny<DateTimeOffset?>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        repoAMock.Verify(r => r.UpdateMessageStatusAsync("ModuleB", It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<DateTimeOffset?>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()), Times.Never);

        repoBMock.Verify(r => r.UpdateMessageStatusAsync("ModuleB", msgB.Id, "Processed", It.IsAny<DateTimeOffset?>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        repoBMock.Verify(r => r.UpdateMessageStatusAsync("ModuleA", It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<DateTimeOffset?>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    // Verification 2a: Singleton Interceptor Concurrency Race / Pollution
    [Fact]
    public async Task SingletonInterceptor_ShouldPolluteAndCauseRace()
    {
        using var connection = new SqliteConnection("Filename=:memory:");
        await connection.OpenAsync();

        var channelMock = new Mock<IOutboxChannel>();
        var moduleSelectorMock = new Mock<IModuleSelector>();
        moduleSelectorMock.Setup(m => m.GetModuleName(It.Is<object>(x => ((VerificationEntity)x).Name == "EntityA"), It.IsAny<IDomainEvent>()))
            .Returns("ModuleA");
        moduleSelectorMock.Setup(m => m.GetModuleName(It.Is<object>(x => ((VerificationEntity)x).Name == "EntityB"), It.IsAny<IDomainEvent>()))
            .Returns("ModuleB");

        var options = new DbContextOptionsBuilder<VerificationDbContext>()
            .UseSqlite(connection)
            .Options;

        using (var context = new VerificationDbContext(options))
        {
            await context.Database.EnsureCreatedAsync();
        }

        using var contextA = new VerificationDbContext(options);
        using var contextB = new VerificationDbContext(options);

        var entityA = new VerificationEntity { Id = Guid.NewGuid(), Name = "EntityA" };
        entityA.DoSomething();
        contextA.VerificationEntities.Add(entityA);

        var entityB = new VerificationEntity { Id = Guid.NewGuid(), Name = "EntityB" };
        entityB.DoSomething();
        contextB.VerificationEntities.Add(entityB);

        var singletonInterceptor = new OutboxSaveChangesInterceptor(channelMock.Object, moduleSelectorMock.Object);

        // Simulate concurrent SavingChanges call (Thread A and Thread B)
        singletonInterceptor.SavingChanges(
            new DbContextEventData((EventDefinitionBase)null!, (d, s) => "", contextA), 
            new InterceptionResult<int>()
        );
        singletonInterceptor.SavingChanges(
            new DbContextEventData((EventDefinitionBase)null!, (d, s) => "", contextB), 
            new InterceptionResult<int>()
        );

        // Now Thread A completes and calls SavedChanges
        singletonInterceptor.SavedChanges(
            new SaveChangesCompletedEventData((EventDefinitionBase)null!, (d, s) => "", contextA, 1), 
            1
        );

        // Assert: Thread A's notification completes, but it POLLUTES and notifies for BOTH ModuleA and ModuleB
        // even though Thread B has not finished/committed yet.
        channelMock.Verify(c => c.WriteAsync("ModuleA", It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(c => c.WriteAsync("ModuleB", It.IsAny<CancellationToken>()), Times.Once);

        channelMock.Invocations.Clear();

        // Thread B completes and calls SavedChanges
        singletonInterceptor.SavedChanges(
            new SaveChangesCompletedEventData((EventDefinitionBase)null!, (d, s) => "", contextB, 1), 
            1
        );

        // Assert: Thread B's notification writes NOTHING to the channel, resulting in a missed/delayed trigger
        channelMock.Verify(c => c.WriteAsync("ModuleB", It.IsAny<CancellationToken>()), Times.Never);
    }

    // Verification 2b: Scoped Interceptor Prevents Concurrency Pollution / Race
    [Fact]
    public async Task ScopedInterceptor_ShouldPreventConcurrencyPollution()
    {
        using var connection = new SqliteConnection("Filename=:memory:");
        await connection.OpenAsync();

        var channelMock = new Mock<IOutboxChannel>();
        
        channelMock.Setup(c => c.WriteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, CancellationToken>((module, ct) => 
            {
                Console.WriteLine($"[DEBUG] WriteAsync called for {module}. StackTrace: {Environment.StackTrace}");
            })
            .Returns(ValueTask.CompletedTask);

        var moduleSelectorMock = new Mock<IModuleSelector>();
        moduleSelectorMock.Setup(m => m.GetModuleName(It.Is<object>(x => ((VerificationEntity)x).Name == "EntityA"), It.IsAny<IDomainEvent>()))
            .Returns("ModuleA");
        moduleSelectorMock.Setup(m => m.GetModuleName(It.Is<object>(x => ((VerificationEntity)x).Name == "EntityB"), It.IsAny<IDomainEvent>()))
            .Returns("ModuleB");

        var options = new DbContextOptionsBuilder<VerificationDbContext>()
            .UseSqlite(connection)
            .Options;

        using (var context = new VerificationDbContext(options))
        {
            await context.Database.EnsureCreatedAsync();
        }

        using var contextA = new VerificationDbContext(options);
        using var contextB = new VerificationDbContext(options);

        var entityA = new VerificationEntity { Id = Guid.NewGuid(), Name = "EntityA" };
        entityA.DoSomething();
        contextA.VerificationEntities.Add(entityA);

        var entityB = new VerificationEntity { Id = Guid.NewGuid(), Name = "EntityB" };
        entityB.DoSomething();
        contextB.VerificationEntities.Add(entityB);

        var scopedInterceptorA = new OutboxSaveChangesInterceptor(channelMock.Object, moduleSelectorMock.Object);
        var scopedInterceptorB = new OutboxSaveChangesInterceptor(channelMock.Object, moduleSelectorMock.Object);

        // Simulate concurrent SavingChanges call (Thread A with scopedInterceptorA, Thread B with scopedInterceptorB)
        scopedInterceptorA.SavingChanges(
            new DbContextEventData((EventDefinitionBase)null!, (d, s) => "", contextA), 
            new InterceptionResult<int>()
        );
        scopedInterceptorB.SavingChanges(
            new DbContextEventData((EventDefinitionBase)null!, (d, s) => "", contextB), 
            new InterceptionResult<int>()
        );

        // Thread A completes and calls SavedChanges
        scopedInterceptorA.SavedChanges(
            new SaveChangesCompletedEventData((EventDefinitionBase)null!, (d, s) => "", contextA, 1), 
            1
        );

        // Assert: ONLY ModuleA is notified. No pollution of ModuleB!
        channelMock.Verify(c => c.WriteAsync("ModuleA", It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(c => c.WriteAsync("ModuleB", It.IsAny<CancellationToken>()), Times.Never);

        channelMock.Invocations.Clear();

        // Thread B completes and calls SavedChanges
        scopedInterceptorB.SavedChanges(
            new SaveChangesCompletedEventData((EventDefinitionBase)null!, (d, s) => "", contextB, 1), 
            1
        );

        // Assert: ModuleB is notified now that its own transaction has completed!
        channelMock.Verify(c => c.WriteAsync("ModuleB", It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(c => c.WriteAsync("ModuleA", It.IsAny<CancellationToken>()), Times.Never);
    }

    // Verification 2c: Scoped Interceptor Concurrency Stress Test
    [Fact]
    public async Task ScopedInterceptor_UnderConcurrentSaves_ShouldPreventRaceConditions()
    {
        using var connection = new SqliteConnection("Filename=:memory:");
        await connection.OpenAsync();

        var channelMock = new Mock<IOutboxChannel>();
        var moduleSelectorMock = new Mock<IModuleSelector>();
        
        moduleSelectorMock.Setup(m => m.GetModuleName(It.IsAny<object>(), It.IsAny<IDomainEvent>()))
            .Returns<object, IDomainEvent>((entity, ev) => 
            {
                return ((VerificationEntity)entity).Name;
            });

        var options = new DbContextOptionsBuilder<VerificationDbContext>()
            .UseSqlite(connection)
            .Options;

        using (var context = new VerificationDbContext(options))
        {
            await context.Database.EnsureCreatedAsync();
        }

        const int concurrentTasksCount = 50;
        var tasks = Enumerable.Range(1, concurrentTasksCount).Select(async i =>
        {
            var moduleName = $"Module_{i}";
            
            var interceptor = new OutboxSaveChangesInterceptor(channelMock.Object, moduleSelectorMock.Object);
            var contextOptions = new DbContextOptionsBuilder<VerificationDbContext>()
                .UseSqlite(connection)
                .AddInterceptors(interceptor)
                .Options;

            using var contextWithInterceptor = new VerificationDbContext(contextOptions);

            var entity = new VerificationEntity { Id = Guid.NewGuid(), Name = moduleName };
            entity.DoSomething();
            contextWithInterceptor.VerificationEntities.Add(entity);

            await contextWithInterceptor.SaveChangesAsync();
        }).ToList();

        await Task.WhenAll(tasks);

        for (int i = 1; i <= concurrentTasksCount; i++)
        {
            var moduleName = $"Module_{i}";
            channelMock.Verify(c => c.WriteAsync(moduleName, It.IsAny<CancellationToken>()), Times.Once);
        }
    }

    // Verification 2d: Empirical OutboxChannel WaitToReadAsync Busy Spin bug verification
    [Fact]
    public async Task OutboxChannel_WhenWrittenOnce_WaitToReadAsyncReturnsTrueRepeatedlyWithoutBlocking_BusySpinBug()
    {
        // Arrange
        var channel = new OutboxChannel();
        var moduleName = "TestModule";

        // Act - Write once
        await channel.WriteAsync(moduleName);

        // Assert - First call to WaitToReadAsync returns true immediately
        var firstWait = await channel.WaitToReadAsync(moduleName);
        Assert.True(firstWait);

        // Second call to WaitToReadAsync also returns true immediately because the item was never read/consumed.
        // This is a known performance/busy-spin bug in the background service polling logic.
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var secondWait = await channel.WaitToReadAsync(moduleName, cts.Token);
        Assert.True(secondWait);
    }

    // Verification 3: Dialect Lock Generation with Custom Schema
    [Theory]
    [InlineData("custom", "OutboxMessages", 100)]
    [InlineData(null, "OutboxMessages", 50)]
    [InlineData("", "OutboxMessages", 50)]
    public void PostgreSqlDialect_LockGeneration_ShouldMatchSchemaExpectations(string? schema, string tableName, int batchSize)
    {
        var dialect = new PostgreSqlDialect();
        var sql = dialect.GetLockMessagesSql(schema, tableName, batchSize);

        if (string.IsNullOrEmpty(schema))
        {
            Assert.Contains($"UPDATE \"{tableName}\"", sql);
            Assert.Contains($"FROM \"{tableName}\"", sql);
        }
        else
        {
            Assert.Contains($"UPDATE \"{schema}\".\"{tableName}\"", sql);
            Assert.Contains($"FROM \"{schema}\".\"{tableName}\"", sql);
        }
    }

    [Theory]
    [InlineData("custom", "OutboxMessages", 100)]
    [InlineData(null, "OutboxMessages", 50)]
    [InlineData("", "OutboxMessages", 50)]
    public void SqlServerDialect_LockGeneration_ShouldMatchSchemaExpectations(string? schema, string tableName, int batchSize)
    {
        var dialect = new SqlServerDialect();
        var sql = dialect.GetLockMessagesSql(schema, tableName, batchSize);

        if (string.IsNullOrEmpty(schema))
        {
            Assert.Contains($"UPDATE [{tableName}]", sql);
            Assert.Contains($"FROM [{tableName}] WITH (UPDLOCK, READPAST)", sql);
        }
        else
        {
            Assert.Contains($"UPDATE [{schema}].[{tableName}]", sql);
            Assert.Contains($"FROM [{schema}].[{tableName}] WITH (UPDLOCK, READPAST)", sql);
        }
    }

    [Theory]
    [InlineData("custom", "OutboxMessages", 100)]
    [InlineData(null, "OutboxMessages", 50)]
    [InlineData("", "OutboxMessages", 50)]
    public void SqliteDialect_LockGeneration_ShouldMatchSchemaExpectations(string? schema, string tableName, int batchSize)
    {
        var dialect = new SqliteDialect();
        var sql = dialect.GetLockMessagesSql(schema, tableName, batchSize);

        if (string.IsNullOrEmpty(schema))
        {
            Assert.Contains($"`{tableName}`", sql);
        }
        else
        {
            Assert.Contains($"`{schema}`.`{tableName}`", sql);
        }
    }
}
