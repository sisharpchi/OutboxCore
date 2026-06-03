using Microsoft.EntityFrameworkCore;
using OutboxCore.EntityFrameworkCore.Extensions;
using OutboxCore.Models;
using OutboxCore.Sample.WebApi.Domain;

namespace OutboxCore.Sample.WebApi.Data;

public class ApplicationDbContext : DbContext
{
    public DbSet<Order> Orders { get; set; } = null!;
    public DbSet<OutboxMessage> OutboxMessages { get; set; } = null!;
    public DbSet<InboxMessage> InboxMessages { get; set; } = null!;

    public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.ApplyOutboxConfigurations();

        modelBuilder.Entity<Order>(builder =>
        {
            builder.ToTable("Orders");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.CustomerName).HasMaxLength(100).IsRequired();
            builder.Property(x => x.TotalAmount).HasPrecision(18, 2);
        });
    }
}
