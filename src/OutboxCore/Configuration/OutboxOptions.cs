using System;
using System.Collections.Generic;

namespace OutboxCore.Configuration;

public class OutboxModuleOptions
{
    public string ModuleName { get; set; } = null!;
    public int BatchSize { get; set; } = 100;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxRetryCount { get; set; } = 5;
    public bool DeleteOnPublish { get; set; } = false;

    public bool EnableCleanup { get; set; } = true;
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);
    public TimeSpan OutboxRetentionPeriod { get; set; } = TimeSpan.FromHours(24);
    public TimeSpan InboxRetentionPeriod { get; set; } = TimeSpan.FromDays(7);
}

public class OutboxOptions
{
    public int BatchSize { get; set; } = 100;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxRetryCount { get; set; } = 5;
    public bool DeleteOnPublish { get; set; } = false;

    public bool EnableCleanup { get; set; } = true;
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);
    public TimeSpan OutboxRetentionPeriod { get; set; } = TimeSpan.FromHours(24);
    public TimeSpan InboxRetentionPeriod { get; set; } = TimeSpan.FromDays(7);

    public List<OutboxModuleOptions> Modules { get; } = new();

    public void RegisterModule(string moduleName, Action<OutboxModuleOptions>? configure = null)
    {
        if (string.IsNullOrEmpty(moduleName))
        {
            throw new ArgumentException("Module name cannot be null or empty.", nameof(moduleName));
        }

        var options = new OutboxModuleOptions
        {
            ModuleName = moduleName,
            BatchSize = BatchSize,
            PollingInterval = PollingInterval,
            LockDuration = LockDuration,
            MaxRetryCount = MaxRetryCount,
            DeleteOnPublish = DeleteOnPublish,
            EnableCleanup = EnableCleanup,
            CleanupInterval = CleanupInterval,
            OutboxRetentionPeriod = OutboxRetentionPeriod,
            InboxRetentionPeriod = InboxRetentionPeriod
        };
        configure?.Invoke(options);
        Modules.Add(options);
    }
}
