using System;

namespace OutboxCore.Abstractions;

public interface IDomainEvent
{
    Guid Id { get; }
    DateTimeOffset OccurredOn { get; }
}
