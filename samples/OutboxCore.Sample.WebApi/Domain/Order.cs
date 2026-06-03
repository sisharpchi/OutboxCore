using System;
using OutboxCore.Models;
using OutboxCore.Sample.WebApi.Events;

namespace OutboxCore.Sample.WebApi.Domain;

public class Order : AggregateRoot
{
    public Guid Id { get; private set; }
    public string CustomerName { get; private set; } = null!;
    public decimal TotalAmount { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }

    // Required by EF Core
    private Order() { }

    public static Order Create(string customerName, decimal totalAmount)
    {
        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerName = customerName,
            TotalAmount = totalAmount,
            CreatedAt = DateTimeOffset.UtcNow
        };

        order.RaiseDomainEvent(new OrderCreatedEvent(order.Id, order.CustomerName, order.TotalAmount));

        return order;
    }
}
