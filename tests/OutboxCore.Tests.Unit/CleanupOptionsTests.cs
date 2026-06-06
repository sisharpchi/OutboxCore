using System;
using OutboxCore.Configuration;
using Xunit;

namespace OutboxCore.Tests.Unit;

public class CleanupOptionsTests
{
    [Fact]
    public void OutboxOptions_DefaultValues_ShouldBeCorrect()
    {
        // Arrange & Act
        var options = new OutboxOptions();

        // Assert
        Assert.True(options.EnableCleanup);
        Assert.Equal(TimeSpan.FromHours(1), options.CleanupInterval);
        Assert.Equal(TimeSpan.FromHours(24), options.OutboxRetentionPeriod);
        Assert.Equal(TimeSpan.FromDays(7), options.InboxRetentionPeriod);
    }

    [Fact]
    public void RegisterModule_ShouldInheritCleanupOptionsFromParent()
    {
        // Arrange
        var parentOptions = new OutboxOptions
        {
            EnableCleanup = false,
            CleanupInterval = TimeSpan.FromMinutes(30),
            OutboxRetentionPeriod = TimeSpan.FromHours(12),
            InboxRetentionPeriod = TimeSpan.FromDays(3)
        };

        // Act
        parentOptions.RegisterModule("TestModule");

        // Assert
        var module = parentOptions.Modules[0];
        Assert.Equal("TestModule", module.ModuleName);
        Assert.False(module.EnableCleanup);
        Assert.Equal(TimeSpan.FromMinutes(30), module.CleanupInterval);
        Assert.Equal(TimeSpan.FromHours(12), module.OutboxRetentionPeriod);
        Assert.Equal(TimeSpan.FromDays(3), module.InboxRetentionPeriod);
    }

    [Fact]
    public void RegisterModule_ShouldOverrideCleanupOptions()
    {
        // Arrange
        var parentOptions = new OutboxOptions
        {
            EnableCleanup = true,
            CleanupInterval = TimeSpan.FromHours(1)
        };

        // Act
        parentOptions.RegisterModule("TestModule", m =>
        {
            m.EnableCleanup = false;
            m.CleanupInterval = TimeSpan.FromMinutes(10);
            m.OutboxRetentionPeriod = TimeSpan.FromMinutes(15);
            m.InboxRetentionPeriod = TimeSpan.FromHours(2);
        });

        // Assert
        var module = parentOptions.Modules[0];
        Assert.Equal("TestModule", module.ModuleName);
        Assert.False(module.EnableCleanup);
        Assert.Equal(TimeSpan.FromMinutes(10), module.CleanupInterval);
        Assert.Equal(TimeSpan.FromMinutes(15), module.OutboxRetentionPeriod);
        Assert.Equal(TimeSpan.FromHours(2), module.InboxRetentionPeriod);
    }
}
