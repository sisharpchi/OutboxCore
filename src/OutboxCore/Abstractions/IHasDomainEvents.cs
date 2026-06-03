using System.Collections.Generic;

namespace OutboxCore.Abstractions;

public interface IHasDomainEvents
{
    IReadOnlyCollection<IDomainEvent> DomainEvents { get; }
    void ClearDomainEvents();
}
