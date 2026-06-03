using System;
using OutboxCore.Abstractions;

namespace OutboxCore.Sample.WebApi.Events;

public class OrderCreatedEvent : IDomainEvent
{
    public Guid Id { get; }
    public Guid OrderId { get; }
    public string CustomerName { get; }
    public decimal TotalAmount { get; }
    public DateTimeOffset OccurredOn { get; }

    public OrderCreatedEvent(Guid orderId, string customerName, decimal totalAmount)
    {
        Id = Guid.NewGuid();
        OrderId = orderId;
        CustomerName = customerName;
        TotalAmount = totalAmount;
        OccurredOn = DateTimeOffset.UtcNow;
    }
}
