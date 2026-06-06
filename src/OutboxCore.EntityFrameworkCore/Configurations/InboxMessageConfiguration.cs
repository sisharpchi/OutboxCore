using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using OutboxCore.Models;

namespace OutboxCore.EntityFrameworkCore.Configurations;

public class InboxMessageConfiguration : IEntityTypeConfiguration<InboxMessage>
{
    public void Configure(EntityTypeBuilder<InboxMessage> builder)
    {
        builder.ToTable("InboxMessages");

        builder.HasKey(x => new { x.Id, x.ModuleName });

        builder.Property(x => x.ModuleName)
            .HasMaxLength(100)
            .IsRequired();

        builder.Property(x => x.MessageType)
            .HasMaxLength(250)
            .IsRequired();

        builder.Property(x => x.Status)
            .HasMaxLength(50)
            .IsRequired();

        builder.HasIndex(x => new { x.ModuleName, x.Status });
    }
}
