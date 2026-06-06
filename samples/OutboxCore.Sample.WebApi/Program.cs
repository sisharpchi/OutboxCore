using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OutboxCore.Configuration;
using OutboxCore.Dialects;
using OutboxCore.EntityFrameworkCore.Extensions;
using OutboxCore.EntityFrameworkCore.Interceptors;
using OutboxCore.RabbitMQ.Extensions;
using OutboxCore.Sample.WebApi.Data;
using OutboxCore.Dashboard;
using OutboxCore.Dapper.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Configure Database Connection (PostgreSQL)
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection") 
                       ?? "Host=localhost;Database=outbox_sample;Username=postgres;Password=postgres";

System.Console.WriteLine($"[DIAGNOSTIC] DB Connection: {connectionString}");
System.Console.WriteLine($"[DIAGNOSTIC] RabbitMQ: {builder.Configuration["RabbitMq:HostName"] ?? "localhost"}:{builder.Configuration["RabbitMq:Port"] ?? "5672"}");

// Register DbContext with the OutboxSaveChangesInterceptor (Keyed for module "Default")
builder.Services.AddDbContext<ApplicationDbContext>((sp, options) =>
{
    options.UseNpgsql(connectionString);
    options.AddInterceptors(sp.GetRequiredKeyedService<OutboxSaveChangesInterceptor>("Default"));
});

// Configure OutboxCore with PostgreSQL and RabbitMQ for module "Default", "Orders", and "Billing"
builder.Services.AddOutboxCore()
    .AddModule("Default", options =>
    {
        options.BatchSize = 50;
        options.PollingInterval = System.TimeSpan.FromSeconds(2);
        options.DeleteOnPublish = false;
    })
    .UseEntityFrameworkCore<ApplicationDbContext>(new PostgreSqlDialect())
    .UseRabbitMq(options =>
    {
        options.HostName = builder.Configuration["RabbitMq:HostName"] ?? "localhost";
        if (int.TryParse(builder.Configuration["RabbitMq:Port"], out var port))
        {
            options.Port = port;
        }
        options.UserName = builder.Configuration["RabbitMq:UserName"] ?? "guest";
        options.Password = builder.Configuration["RabbitMq:Password"] ?? "guest";
    })
    .And()
    .AddModule("Orders", options =>
    {
        options.BatchSize = 50;
        options.PollingInterval = System.TimeSpan.FromSeconds(2);
        options.DeleteOnPublish = false;
    })
    .UseEntityFrameworkCore<ApplicationDbContext>(new PostgreSqlDialect())
    .UseRabbitMq(options =>
    {
        options.HostName = builder.Configuration["RabbitMq:HostName"] ?? "localhost";
        if (int.TryParse(builder.Configuration["RabbitMq:Port"], out var port))
        {
            options.Port = port;
        }
        options.UserName = builder.Configuration["RabbitMq:UserName"] ?? "guest";
        options.Password = builder.Configuration["RabbitMq:Password"] ?? "guest";
    })
    .And()
    .AddModule("Billing", options =>
    {
        options.BatchSize = 50;
        options.PollingInterval = System.TimeSpan.FromSeconds(2);
        options.DeleteOnPublish = false;
    })
    .UseDapper(sp => new Npgsql.NpgsqlConnection(connectionString), new PostgreSqlDialect())
    .UseRabbitMq(options =>
    {
        options.HostName = builder.Configuration["RabbitMq:HostName"] ?? "localhost";
        if (int.TryParse(builder.Configuration["RabbitMq:Port"], out var port))
        {
            options.Port = port;
        }
        options.UserName = builder.Configuration["RabbitMq:UserName"] ?? "guest";
        options.Password = builder.Configuration["RabbitMq:Password"] ?? "guest";
    });

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();
app.MapControllers();

app.UseOutboxDashboard();

// Ensure the database is created and initialized on startup
using (var scope = app.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
    dbContext.Database.EnsureCreated();
}

app.Run();

public partial class Program { }
