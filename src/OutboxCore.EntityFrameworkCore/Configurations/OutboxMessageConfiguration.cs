using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using OutboxCore.Models;

namespace OutboxCore.EntityFrameworkCore.Configurations;

public class OutboxMessageConfiguration : IEntityTypeConfiguration<OutboxMessage>
{
    public void Configure(EntityTypeBuilder<OutboxMessage> builder)
    {
        builder.ToTable("OutboxMessages");

        builder.HasKey(x => x.Id);

        builder.Property(x => x.ModuleName)
            .HasMaxLength(100)
            .IsRequired();

        builder.Property(x => x.MessageType)
            .HasMaxLength(250)
            .IsRequired();

        builder.Property(x => x.Content)
            .IsRequired();

        builder.Property(x => x.Status)
            .HasMaxLength(50)
            .IsRequired();

        builder.Property(x => x.WorkerId)
            .HasMaxLength(100);

        builder.HasIndex(x => new { x.ModuleName, x.Status });
        builder.HasIndex(x => new { x.ModuleName, x.Status, x.CreatedAt });
    }
}
