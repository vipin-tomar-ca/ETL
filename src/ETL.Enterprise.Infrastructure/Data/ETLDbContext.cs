using Microsoft.EntityFrameworkCore;
using ETL.Enterprise.Domain.Entities;

namespace ETL.Enterprise.Infrastructure.Data;

/// <summary>
/// Entity Framework DbContext for ETL operations
/// </summary>
public class ETLDbContext : DbContext
{
    public ETLDbContext(DbContextOptions<ETLDbContext> options) : base(options)
    {
    }
    
    public DbSet<ETLJob> ETLJobs { get; set; }
    public DbSet<ETLJobLog> ETLJobLogs { get; set; }
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        
        // Configure ETLJob entity
        modelBuilder.Entity<ETLJob>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).IsRequired().HasMaxLength(100);
            entity.Property(e => e.Description).HasMaxLength(500);
            entity.Property(e => e.Status).IsRequired();
            entity.Property(e => e.CreatedAt).IsRequired();
            entity.Property(e => e.ErrorMessage).HasMaxLength(1000);
            
            // Configure complex types
            entity.OwnsOne(e => e.Configuration, config =>
            {
                config.OwnsOne(c => c.Source);
                config.OwnsOne(c => c.Target);
                config.OwnsOne(c => c.Transformation);
                config.OwnsOne(c => c.Scheduling);
                config.OwnsOne(c => c.ErrorHandling);
                config.OwnsOne(c => c.Performance);
            });
            
            // Configure logs relationship
            entity.HasMany(e => e.Logs)
                  .WithOne()
                  .HasForeignKey(l => l.ETLJobId)
                  .OnDelete(DeleteBehavior.Cascade);
        });
        
        // Configure ETLJobLog entity
        modelBuilder.Entity<ETLJobLog>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.ETLJobId).IsRequired();
            entity.Property(e => e.Timestamp).IsRequired();
            entity.Property(e => e.Level).IsRequired();
            entity.Property(e => e.Message).IsRequired();
            entity.Property(e => e.Exception).HasMaxLength(4000);
            entity.Property(e => e.StackTrace).HasMaxLength(8000);
            
            // Configure JSON properties
            entity.Property(e => e.Properties)
                  .HasConversion(
                      v => System.Text.Json.JsonSerializer.Serialize(v, (System.Text.Json.JsonSerializerOptions?)null),
                      v => System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(v, (System.Text.Json.JsonSerializerOptions?)null) ?? new Dictionary<string, string>()
                  );
        });
        
        // Create indexes for better performance
        modelBuilder.Entity<ETLJob>()
            .HasIndex(e => e.Status);
            
        modelBuilder.Entity<ETLJob>()
            .HasIndex(e => e.CreatedAt);
            
        modelBuilder.Entity<ETLJob>()
            .HasIndex(e => e.Name);
            
        modelBuilder.Entity<ETLJobLog>()
            .HasIndex(e => e.ETLJobId);
            
        modelBuilder.Entity<ETLJobLog>()
            .HasIndex(e => e.Timestamp);
            
        modelBuilder.Entity<ETLJobLog>()
            .HasIndex(e => e.Level);
    }
}
