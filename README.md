# OutboxCore

[![Build and Test](https://github.com/sisharpchi/OutboxCore/actions/workflows/ci.yml/badge.svg)](https://github.com/sisharpchi/OutboxCore/actions/workflows/ci.yml)
[![NuGet Version](https://img.shields.io/nuget/v/OutboxCore.svg)](https://www.nuget.org/packages/OutboxCore)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

OutboxCore is a high-performance, lightweight, and extensible transactional messaging library for the modern .NET ecosystem. It solves the distributed transaction problem by providing reliable **Transactional Outbox** (reliable publishing) and **Transactional Inbox** (idempotent consumption) patterns for your microservices and modular monoliths.

Built with performance, low memory allocations, and scalability in mind, it is fully compatible with **Native AOT** and targets high-throughput distributed systems.

---

## ✨ Features

- 🔄 **Transactional Outbox Pattern**: Ensures domain events are written atomically with your business entities in the same database transaction.
- 📥 **Transactional Inbox Pattern**: Enforces idempotent message consumption, eliminating duplicate message processing in at-least-once message brokers.
- ⚡ **High Throughput (Low Allocation)**: Utilizes `System.Threading.Channels` for efficient, lock-free in-memory batch processing and event dispatching.
- 🔌 **Database Agnostic (EF Core & Dapper)**: Optimized SQL locking dialects (`ISqlDialect`) like `FOR UPDATE SKIP LOCKED` for PostgreSQL and `UPDLOCK, READPAST` for SQL Server, enabling seamless scaling across replicas.
- 🧱 **Modular Monolith Isolation**: Supports independent outbox/inbox processing loops partitioned by module name or schema prefix (e.g., `orders`, `billing`).
- 🧹 **Automatic Storage Pruning**: Background cleanup services periodically prune old processed outbox and inbox records based on module retention options.
- 🎛️ **Glassmorphic Dashboard UI**: Real-time analytics dashboard served directly from your ASP.NET Core application via `app.UseOutboxDashboard()`, displaying metrics and offering manual message retries.
- 📦 **Multiple Broker Transports**: Full support for RabbitMQ and Apache Kafka publishers.
- 🚀 **Native AOT Ready**: Completely reflection-free, utilizing `System.Text.Json` Source Generators for blazing-fast serialization.

---

## 🚀 System Architecture

```
                                  +-----------------------------------------+
                                  |         OutboxCore.Sample.WebApi        |
                                  +-----------------------------------------+
                                 /                     |                     \
                                /                      |                      \
                               v                       v                       v
               +-------------------+         +-------------------+         +-------------------+
               |    OutboxCore     |         | OutboxCore.EFCore |         | OutboxCore.Dapper |
               +-------------------+         +-------------------+         +-------------------+
               | Core Contracts    |         | DbContext         |         | Dapper Repos      |
               | Background Worker |         | Interceptors      |         | Raw SQL Dialects  |
               | Dashboard UI      |         | DB Configurations |         +-------------------+
               +-------------------+         +-------------------+                  
                                 \                     /
                                  v                   v
                     +-------------------+     +-------------------+
                     |OutboxCore.RabbitMQ|     | OutboxCore.Kafka  |
                     +-------------------+     +-------------------+
                     | RabbitMQ Pub      |     | Kafka Pub         |
                     +-------------------+     +-------------------+
```

---

## 📦 Installation

Install the core library and the adapters you need via NuGet Package Manager:

```bash
# Core Abstractions, Background Worker, and Dashboard UI
dotnet add package OutboxCore --version 1.1.1

# Entity Framework Core Integration
dotnet add package OutboxCore.EntityFrameworkCore --version 1.1.1

# Dapper Integration
dotnet add package OutboxCore.Dapper --version 1.1.1

# RabbitMQ Transport Provider
dotnet add package OutboxCore.RabbitMQ --version 1.1.1

# Kafka Transport Provider
dotnet add package OutboxCore.Kafka --version 1.1.1
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

#### Single Module Setup (EF Core + RabbitMQ)
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
    options.AddInterceptors(sp.GetRequiredKeyService<OutboxSaveChangesInterceptor>("Default"));
});

// Configure OutboxCore
builder.Services.AddOutboxCore(options =>
{
    options.BatchSize = 100;
    options.PollingInterval = TimeSpan.FromSeconds(5);
    options.DeleteOnPublish = false; // Set to true to delete messages instead of keeping them
    options.EnableCleanup = true;    // Periodically prune old processed records
    options.CleanupInterval = TimeSpan.FromHours(1);
    options.OutboxRetentionPeriod = TimeSpan.FromHours(24);
    options.InboxRetentionPeriod = TimeSpan.FromDays(7);
})
.UseEntityFrameworkCore<ApplicationDbContext>(new PostgreSqlDialect()) // Inject DB Dialect
.UseRabbitMq(options =>
{
    options.HostName = builder.Configuration["RabbitMq:HostName"] ?? "localhost";
    options.UserName = builder.Configuration["RabbitMq:UserName"] ?? "guest";
    options.Password = builder.Configuration["RabbitMq:Password"] ?? "guest";
});
```

#### Modular Monolith Multi-Module Setup (Dapper + Kafka + EF Core)
You can configure isolated outbox and inbox workers per module with their own transport and repository settings:
```csharp
using OutboxCore.Configuration;
using OutboxCore.Dapper.Extensions;
using OutboxCore.Dialects;
using OutboxCore.EntityFrameworkCore.Extensions;
using OutboxCore.Kafka.Extensions;
using OutboxCore.RabbitMQ.Extensions;

builder.Services.AddOutboxCore()
    // 1. Configure the "Orders" module using EF Core and RabbitMQ
    .AddModule("Orders", moduleOptions =>
    {
        moduleOptions.BatchSize = 50;
        moduleOptions.PollingInterval = TimeSpan.FromSeconds(2);
        moduleOptions.OutboxRetentionPeriod = TimeSpan.FromHours(12);
    })
    .UseEntityFrameworkCore<ApplicationDbContext>(new PostgreSqlDialect())
    .UseRabbitMq(options => { /* RabbitMQ config */ })
    
    // 2. Configure the "Billing" module using Dapper and Apache Kafka
    .AddModule("Billing", moduleOptions =>
    {
        moduleOptions.BatchSize = 200;
        moduleOptions.PollingInterval = TimeSpan.FromSeconds(1);
        moduleOptions.InboxRetentionPeriod = TimeSpan.FromDays(3);
    })
    .UseDapper(sp => sp.GetRequiredService<DbConnection>(), new PostgreSqlDialect())
    .UseKafka(options =>
    {
        options.BootstrapServers = "localhost:9092";
    });
```

### 3. Register the Glassmorphic Dashboard UI

Register the lightweight embedded web dashboard middleware in your application pipeline:

```csharp
using OutboxCore.Dashboard;

var app = builder.Build();

// Enable the real-time outbox/inbox dashboard
app.UseOutboxDashboard("/outbox-dashboard");

app.Run();
```

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

3. **E2E Tests** (runs against SQLite in-memory fallback, or PostgreSQL and RabbitMQ Testcontainers if Docker is active):
   ```bash
   dotnet test tests/OutboxCore.Tests.E2e/OutboxCore.Tests.E2e.csproj
   ```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.