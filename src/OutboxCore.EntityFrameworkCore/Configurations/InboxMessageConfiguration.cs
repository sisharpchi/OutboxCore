using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using OutboxCore.Models;

namespace OutboxCore.EntityFrameworkCore.Configurations;

public class InboxMessageConfiguration : IEntityTypeConfiguration<InboxMessage>
{
    public void Configure(EntityTypeBuilder<InboxMessage> builder)
    {
        builder.ToTable("InboxMessages");

        builder.HasKey(x => x.Id);

        builder.Property(x => x.MessageType)
            .HasMaxLength(250)
            .IsRequired();

        builder.Property(x => x.Status)
            .HasMaxLength(50)
            .IsRequired();

        builder.HasIndex(x => x.Status);
    }
}
