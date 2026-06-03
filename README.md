# OutboxCore

[![Build and Test](https://github.com/sisharpchi/OutboxCore/actions/workflows/ci.yml/badge.svg)](https://github.com/sisharpchi/OutboxCore/actions/workflows/ci.yml)
[![NuGet Version](https://img.shields.io/nuget/v/OutboxCore.svg)](https://www.nuget.org/packages/OutboxCore)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

OutboxCore is a high-performance, lightweight, and extensible transactional messaging library for the modern .NET ecosystem. It solves the distributed transaction problem by providing reliable **Transactional Outbox** (reliable publishing) and **Transactional Inbox** (idempotent consumption) patterns for your microservices.

Built with performance, low memory allocations, and scalability in mind, it is fully compatible with **Native AOT** and targets high-throughput distributed systems.

---

## ✨ Features

- 🔄 **Transactional Outbox Pattern**: Ensures domain events are written atomically with your business entities in the same database transaction.
- 📥 **Transactional Inbox Pattern**: Enforces idempotent message consumption, eliminating duplicate message processing in at-least-once message brokers.
- ⚡ **High Throughput (Low Allocation)**: Utilizes `System.Threading.Channels` for efficient, lock-free in-memory batch processing and event dispatching.
- 🔌 **Database Agnostic (Optimized Locks)**: Features database-specific SQL locking dialects (`ISqlDialect`) like `FOR UPDATE SKIP LOCKED` for PostgreSQL and `UPDLOCK, READPAST` for SQL Server, enabling seamless scaling across replicas.
- 📦 **Native AOT Ready**: Completely reflection-free, utilizing `System.Text.Json` Source Generators for blazing-fast serialization.
- 🛠️ **Fluent Configuration**: A modern C# builder pattern for clean dependency injection and startup registration.

---

## 🚀 System Architecture

```
                                  +-----------------------------------------+
                                  |         OutboxCore.Sample.WebApi        |
                                  +-----------------------------------------+
                                       /                  |                \
                                      /                   |                 \
                                     v                    v                  v
                     +-------------------+      +-------------------+      +-------------------+
                     |    OutboxCore     |      | OutboxCore.EFCore |      |OutboxCore.RabbitMQ|
                     +-------------------+      +-------------------+      +-------------------+
                     | Core Contracts    |      | DbContext         |      | RabbitMQ          |
                     | Background Worker |      | Interceptors      |      | Message Publisher |
                     | SQL Dialect Engine|      | DB Configurations |      | & Consumer        |
                     +-------------------+      +-------------------+      +-------------------+
```

---

## 📦 Installation

Install the core library and the adapters you need via NuGet Package Manager:

```bash
# Core Abstractions and Background Worker
dotnet add package OutboxCore

# Entity Framework Core Integration
dotnet add package OutboxCore.EntityFrameworkCore

# RabbitMQ Transport Provider
dotnet add package OutboxCore.RabbitMQ
```

---

## 🛠️ Getting Started

### 1. Configure EF Core DbContext

Apply the Outbox configurations and register the Interceptor inside your `DbContext`:

```csharp
using Microsoft.EntityFrameworkCore;
using OutboxCore.EntityFrameworkCore.Extensions;
using OutboxCore.Models;

public class ApplicationDbContext : DbContext
{
    public DbSet<Order> Orders { get; set; } = null!;
    
    // Outbox & Inbox DbSets are required
    public DbSet<OutboxMessage> OutboxMessages { get; set; } = null!;
    public DbSet<InboxMessage> InboxMessages { get; set; } = null!;

    public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Apply Outbox & Inbox database schema configurations
        modelBuilder.ApplyOutboxConfigurations();
    }
}
```

### 2. Configure Dependency Injection in `Program.cs`

Register `OutboxCore` with PostgreSQL locking and RabbitMQ publishing:

```csharp
using OutboxCore.Configuration;
using OutboxCore.Dialects;
using OutboxCore.EntityFrameworkCore.Extensions;
using OutboxCore.EntityFrameworkCore.Interceptors;
using OutboxCore.RabbitMQ.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Register DbContext with the OutboxSaveChangesInterceptor
builder.Services.AddDbContext<ApplicationDbContext>((sp, options) =>
{
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection"));
    options.AddInterceptors(sp.GetRequiredService<OutboxSaveChangesInterceptor>());
});

// Configure OutboxCore
builder.Services.AddOutboxCore(options =>
{
    options.BatchSize = 100;
    options.PollingInterval = TimeSpan.FromSeconds(5);
    options.DeleteOnPublish = false; // Set to true to delete messages instead of marking as Processed
})
.UseEntityFrameworkCore<ApplicationDbContext>(new PostgreSqlDialect()) // Inject DB Dialect
.UseRabbitMq(options =>
{
    options.HostName = builder.Configuration["RabbitMq:HostName"] ?? "localhost";
    options.UserName = builder.Configuration["RabbitMq:UserName"] ?? "guest";
    options.Password = builder.Configuration["RabbitMq:Password"] ?? "guest";
});
```

### 3. Emitting Domain Events from Entities

Inherit your entities from `AggregateRoot` and raise domain events during business logic execution:

```csharp
using OutboxCore.Models;

public class Order : AggregateRoot
{
    public Guid Id { get; private set; }
    public string CustomerName { get; private set; } = null!;
    public decimal TotalAmount { get; private set; }

    public static Order Create(string customerName, decimal totalAmount)
    {
        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerName = customerName,
            TotalAmount = totalAmount
        };

        // Raise domain event
        order.RaiseDomainEvent(new OrderCreatedEvent(order.Id, order.CustomerName, order.TotalAmount));

        return order;
    }
}
```

When you call `SaveChanges` or `SaveChangesAsync` on your `DbContext`, `OutboxSaveChangesInterceptor` will automatically intercept the aggregates, extract the domain events, serialize them to JSON, and save them in the same transaction as your Order insert.

Once successfully saved, the background worker will be triggered via `System.Threading.Channels` for near real-time delivery!

---

## 🧪 Running Tests

To run the automated tests locally:

1. **Unit Tests** (no infrastructure required):
   ```bash
   dotnet test tests/OutboxCore.Tests.Unit/OutboxCore.Tests.Unit.csproj
   ```

2. **Integration Tests** (requires Docker running):
   ```bash
   dotnet test tests/OutboxCore.Tests.Integration/OutboxCore.Tests.Integration.csproj
   ```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.