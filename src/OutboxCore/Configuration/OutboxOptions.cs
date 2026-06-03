using System;

namespace OutboxCore.Configuration;

public class OutboxOptions
{
    public int BatchSize { get; set; } = 100;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxRetryCount { get; set; } = 5;
    public bool DeleteOnPublish { get; set; } = false;
}
