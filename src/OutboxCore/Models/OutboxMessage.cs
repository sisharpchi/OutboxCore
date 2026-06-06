using System;

namespace OutboxCore.Models;

public class OutboxMessage
{
    public Guid Id { get; set; }
    public string ModuleName { get; set; } = null!;
    public string MessageType { get; set; } = null!;
    public string Content { get; set; } = null!;
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? ProcessedAt { get; set; }
    public string Status { get; set; } = "Pending"; // Pending, Processing, Processed, Failed
    public DateTimeOffset? LockedUntil { get; set; }
    public string? WorkerId { get; set; }
    public string? Error { get; set; }
    public int RetryCount { get; set; }
}
