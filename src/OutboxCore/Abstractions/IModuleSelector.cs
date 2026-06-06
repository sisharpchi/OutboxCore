using System;

namespace OutboxCore.Abstractions;

public interface IModuleSelector
{
    string GetModuleName(object entity, IDomainEvent domainEvent);
}
