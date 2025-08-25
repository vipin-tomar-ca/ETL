using System.ComponentModel.DataAnnotations;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Entities;

/// <summary>
/// Represents a processing job in the multi-tenant ETL workflow
/// </summary>
public class ProcessingJob
{
    public Guid Id { get; private set; }
    
    public Guid TenantId { get; private set; }
    
    [Required]
    [StringLength(100)]
    public string Name { get; private set; } = string.Empty;
    
    [StringLength(500)]
    public string Description { get; private set; } = string.Empty;
    
    public ProcessingJobType JobType { get; private set; }
    
    public ProcessingJobStatus Status { get; private set; }
    
    public DateTime CreatedAt { get; private set; }
    
    public DateTime? StartedAt { get; private set; }
    
    public DateTime? CompletedAt { get; private set; }
    
    public int RecordsProcessed { get; private set; }
    
    public int RecordsFailed { get; private set; }
    
    [StringLength(1000)]
    public string? ErrorMessage { get; private set; }
    
    public ProcessingJobConfiguration Configuration { get; private set; } = new();
    
    public List<ProcessingJobStep> Steps { get; private set; } = new();
    
    public List<ProcessingJobLog> Logs { get; private set; } = new();
    
    // Navigation properties
    public Tenant Tenant { get; private set; } = null!;
    public ETLJob? ETLJob { get; private set; }
    
    // Private constructor for EF Core
    private ProcessingJob() { }
    
    public ProcessingJob(Guid tenantId, string name, string description, ProcessingJobType jobType, ProcessingJobConfiguration? configuration = null)
    {
        Id = Guid.NewGuid();
        TenantId = tenantId;
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Description = description ?? string.Empty;
        JobType = jobType;
        Configuration = configuration ?? new ProcessingJobConfiguration();
        Status = ProcessingJobStatus.Pending;
        CreatedAt = DateTime.UtcNow;
    }
    
    public void Start()
    {
        if (Status != ProcessingJobStatus.Pending)
            throw new InvalidOperationException("Job can only be started from Pending status");
        
        Status = ProcessingJobStatus.Running;
        StartedAt = DateTime.UtcNow;
        AddLog("Job started", ProcessingJobLogLevel.Information);
    }
    
    public void Complete(int recordsProcessed, int recordsFailed = 0)
    {
        if (Status != ProcessingJobStatus.Running)
            throw new InvalidOperationException("Job can only be completed from Running status");
        
        Status = ProcessingJobStatus.Completed;
        CompletedAt = DateTime.UtcNow;
        RecordsProcessed = recordsProcessed;
        RecordsFailed = recordsFailed;
        
        var message = $"Job completed. Processed: {recordsProcessed}, Failed: {recordsFailed}";
        AddLog(message, ProcessingJobLogLevel.Information);
    }
    
    public void Fail(string errorMessage)
    {
        if (Status != ProcessingJobStatus.Running)
            throw new InvalidOperationException("Job can only be failed from Running status");
        
        Status = ProcessingJobStatus.Failed;
        CompletedAt = DateTime.UtcNow;
        ErrorMessage = errorMessage;
        
        AddLog($"Job failed: {errorMessage}", ProcessingJobLogLevel.Error);
    }
    
    public void Cancel()
    {
        if (Status != ProcessingJobStatus.Running && Status != ProcessingJobStatus.Pending)
            throw new InvalidOperationException("Job can only be cancelled from Running or Pending status");
        
        Status = ProcessingJobStatus.Cancelled;
        CompletedAt = DateTime.UtcNow;
        
        AddLog("Job cancelled", ProcessingJobLogLevel.Warning);
    }
    
    public void Pause()
    {
        if (Status != ProcessingJobStatus.Running)
            throw new InvalidOperationException("Job can only be paused from Running status");
        
        Status = ProcessingJobStatus.Paused;
        AddLog("Job paused", ProcessingJobLogLevel.Warning);
    }
    
    public void Resume()
    {
        if (Status != ProcessingJobStatus.Paused)
            throw new InvalidOperationException("Job can only be resumed from Paused status");
        
        Status = ProcessingJobStatus.Running;
        AddLog("Job resumed", ProcessingJobLogLevel.Information);
    }
    
    public void UpdateProgress(int recordsProcessed, int recordsFailed = 0)
    {
        RecordsProcessed = recordsProcessed;
        RecordsFailed = recordsFailed;
    }
    
    public void AddLog(string message, ProcessingJobLogLevel level)
    {
        var log = new ProcessingJobLog(Id, message, level);
        Logs.Add(log);
    }
    
    public void AddStep(ProcessingJobStep step)
    {
        if (step == null) throw new ArgumentNullException(nameof(step));
        Steps.Add(step);
    }
    
    public void UpdateConfiguration(ProcessingJobConfiguration configuration)
    {
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }
}

/// <summary>
/// Configuration for a processing job
/// </summary>
public class ProcessingJobConfiguration
{
    public DataSourceConfiguration DataSource { get; set; } = new();
    public DataTargetConfiguration DataTarget { get; set; } = new();
    public TransformationConfiguration? Transformation { get; set; }
    public FileExportConfiguration FileExport { get; set; } = new();
    public SFTPConfiguration SFTP { get; set; } = new();
    public UserManagementConfiguration UserManagement { get; set; } = new();
    public SchedulingConfiguration Scheduling { get; set; } = new();
    public ErrorHandlingConfiguration ErrorHandling { get; set; } = new();
}

/// <summary>
/// File export configuration
/// </summary>
public class FileExportConfiguration
{
    public FileFormat Format { get; set; } = FileFormat.CSV;
    public string OutputDirectory { get; set; } = string.Empty;
    public string FileNamePattern { get; set; } = "{TenantCode}_{JobType}_{Timestamp}";
    public bool IncludeHeaders { get; set; } = true;
    public string Delimiter { get; set; } = ",";
    public bool EnableCompression { get; set; } = true;
    public int MaxFileSizeMB { get; set; } = 100;
    public bool SplitLargeFiles { get; set; } = true;
}


