using System;

namespace OutboxCore.Models;

public class InboxMessage
{
    public Guid Id { get; set; }
    public string MessageType { get; set; } = null!;
    public DateTimeOffset ReceivedAt { get; set; }
    public DateTimeOffset? ProcessedAt { get; set; }
    public string Status { get; set; } = "Pending"; // Pending, Processing, Processed, Failed
    public string? Error { get; set; }
}
