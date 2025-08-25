using System.ComponentModel.DataAnnotations;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Entities;

/// <summary>
/// Represents an ETL job with its configuration and execution details
/// </summary>
public class ETLJob
{
    public Guid Id { get; private set; }
    
    [Required]
    [StringLength(100)]
    public string Name { get; private set; } = string.Empty;
    
    [StringLength(500)]
    public string Description { get; private set; } = string.Empty;
    
    public ETLJobStatus Status { get; private set; }
    
    public DateTime CreatedAt { get; private set; }
    
    public DateTime? StartedAt { get; private set; }
    
    public DateTime? CompletedAt { get; private set; }
    
    public int RecordsProcessed { get; private set; }
    
    public int RecordsFailed { get; private set; }
    
    [StringLength(1000)]
    public string? ErrorMessage { get; private set; }
    
    public ETLJobConfiguration Configuration { get; private set; } = new();
    
    public List<ETLJobLog> Logs { get; private set; } = new();
    
    // Private constructor for EF Core
    private ETLJob() { }
    
    public ETLJob(string name, string description, ETLJobConfiguration configuration)
    {
        Id = Guid.NewGuid();
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Description = description ?? string.Empty;
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        Status = ETLJobStatus.Created;
        CreatedAt = DateTime.UtcNow;
    }
    
    public void Start()
    {
        if (Status != ETLJobStatus.Created && Status != ETLJobStatus.Failed)
            throw new InvalidOperationException($"Cannot start job in {Status} status");
            
        Status = ETLJobStatus.Running;
        StartedAt = DateTime.UtcNow;
        AddLog("Job started", ETLJobLogLevel.Information);
    }
    
    public void Complete(int recordsProcessed, int recordsFailed = 0)
    {
        if (Status != ETLJobStatus.Running)
            throw new InvalidOperationException($"Cannot complete job in {Status} status");
            
        Status = ETLJobStatus.Completed;
        CompletedAt = DateTime.UtcNow;
        RecordsProcessed = recordsProcessed;
        RecordsFailed = recordsFailed;
        AddLog($"Job completed. Processed: {recordsProcessed}, Failed: {recordsFailed}", ETLJobLogLevel.Information);
    }
    
    public void Fail(string errorMessage)
    {
        if (Status != ETLJobStatus.Running)
            throw new InvalidOperationException($"Cannot fail job in {Status} status");
            
        Status = ETLJobStatus.Failed;
        CompletedAt = DateTime.UtcNow;
        ErrorMessage = errorMessage;
        AddLog($"Job failed: {errorMessage}", ETLJobLogLevel.Error);
    }
    
    public void AddLog(string message, ETLJobLogLevel level)
    {
        var log = new ETLJobLog(Id, message, level);
        Logs.Add(log);
    }
    
    public void UpdateProgress(int recordsProcessed, int recordsFailed = 0)
    {
        RecordsProcessed = recordsProcessed;
        RecordsFailed = recordsFailed;
        AddLog($"Progress update - Processed: {recordsProcessed}, Failed: {recordsFailed}", ETLJobLogLevel.Debug);
    }
}
