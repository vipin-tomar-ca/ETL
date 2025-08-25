using System.ComponentModel.DataAnnotations;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Entities;

/// <summary>
/// Represents a step in a processing job workflow
/// </summary>
public class ProcessingJobStep
{
    public Guid Id { get; private set; }
    
    public Guid ProcessingJobId { get; private set; }
    
    [Required]
    [StringLength(100)]
    public string Name { get; private set; } = string.Empty;
    
    [StringLength(500)]
    public string Description { get; private set; } = string.Empty;
    
    public ProcessingStepType StepType { get; private set; }
    
    public int Order { get; private set; }
    
    public ProcessingStepStatus Status { get; private set; }
    
    public DateTime CreatedAt { get; private set; }
    
    public DateTime? StartedAt { get; private set; }
    
    public DateTime? CompletedAt { get; private set; }
    
    public int RecordsProcessed { get; private set; }
    
    public int RecordsFailed { get; private set; }
    
    [StringLength(1000)]
    public string? ErrorMessage { get; private set; }
    
    public ProcessingStepConfiguration Configuration { get; private set; } = new();
    
    public List<ProcessingStepLog> Logs { get; private set; } = new();
    
    // Navigation property
    public ProcessingJob ProcessingJob { get; private set; } = null!;
    
    // Private constructor for EF Core
    private ProcessingJobStep() { }
    
    public ProcessingJobStep(Guid processingJobId, string name, string description, ProcessingStepType stepType, int order, ProcessingStepConfiguration? configuration = null)
    {
        Id = Guid.NewGuid();
        ProcessingJobId = processingJobId;
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Description = description ?? string.Empty;
        StepType = stepType;
        Order = order;
        Configuration = configuration ?? new ProcessingStepConfiguration();
        Status = ProcessingStepStatus.Pending;
        CreatedAt = DateTime.UtcNow;
    }
    
    public void Start()
    {
        if (Status != ProcessingStepStatus.Pending)
            throw new InvalidOperationException("Step can only be started from Pending status");
        
        Status = ProcessingStepStatus.Running;
        StartedAt = DateTime.UtcNow;
        AddLog("Step started", ProcessingStepLogLevel.Information);
    }
    
    public void Complete(int recordsProcessed, int recordsFailed = 0)
    {
        if (Status != ProcessingStepStatus.Running)
            throw new InvalidOperationException("Step can only be completed from Running status");
        
        Status = ProcessingStepStatus.Completed;
        CompletedAt = DateTime.UtcNow;
        RecordsProcessed = recordsProcessed;
        RecordsFailed = recordsFailed;
        
        var message = $"Step completed. Processed: {recordsProcessed}, Failed: {recordsFailed}";
        AddLog(message, ProcessingStepLogLevel.Information);
    }
    
    public void Fail(string errorMessage)
    {
        if (Status != ProcessingStepStatus.Running)
            throw new InvalidOperationException("Step can only be failed from Running status");
        
        Status = ProcessingStepStatus.Failed;
        CompletedAt = DateTime.UtcNow;
        ErrorMessage = errorMessage;
        
        AddLog($"Step failed: {errorMessage}", ProcessingStepLogLevel.Error);
    }
    
    public void Skip()
    {
        if (Status != ProcessingStepStatus.Pending)
            throw new InvalidOperationException("Step can only be skipped from Pending status");
        
        Status = ProcessingStepStatus.Skipped;
        CompletedAt = DateTime.UtcNow;
        
        AddLog("Step skipped", ProcessingStepLogLevel.Warning);
    }
    
    public void UpdateProgress(int recordsProcessed, int recordsFailed = 0)
    {
        RecordsProcessed = recordsProcessed;
        RecordsFailed = recordsFailed;
    }
    
    public void AddLog(string message, ProcessingStepLogLevel level)
    {
        var log = new ProcessingStepLog(Id, message, level);
        Logs.Add(log);
    }
    
    public void UpdateConfiguration(ProcessingStepConfiguration configuration)
    {
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }
}

/// <summary>
/// Configuration for a processing step
/// </summary>
public class ProcessingStepConfiguration
{
    public Dictionary<string, string> Parameters { get; set; } = new();
    public TimeSpan Timeout { get; set; } = TimeSpan.FromMinutes(30);
    public bool IsRequired { get; set; } = true;
    public bool ContinueOnError { get; set; } = false;
    public int RetryAttempts { get; set; } = 3;
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMinutes(1);
    public Dictionary<string, object> CustomSettings { get; set; } = new();
}

/// <summary>
/// Log entry for a processing step
/// </summary>
public class ProcessingStepLog
{
    public Guid Id { get; private set; }
    public Guid ProcessingStepId { get; private set; }
    public DateTime Timestamp { get; private set; }
    public ProcessingStepLogLevel Level { get; private set; }
    public string Message { get; private set; } = string.Empty;
    public string? Exception { get; private set; }
    public string? StackTrace { get; private set; }
    public Dictionary<string, string>? Properties { get; private set; }
    
    // Navigation property
    public ProcessingJobStep ProcessingStep { get; private set; } = null!;
    
    // Private constructor for EF Core
    private ProcessingStepLog() { }
    
    public ProcessingStepLog(Guid processingStepId, string message, ProcessingStepLogLevel level)
    {
        Id = Guid.NewGuid();
        ProcessingStepId = processingStepId;
        Message = message ?? throw new ArgumentNullException(nameof(message));
        Level = level;
        Timestamp = DateTime.UtcNow;
    }
    
    public ProcessingStepLog(Guid processingStepId, string message, ProcessingStepLogLevel level, Exception exception)
        : this(processingStepId, message, level)
    {
        Exception = exception.Message;
        StackTrace = exception.StackTrace;
    }
    
    public ProcessingStepLog(Guid processingStepId, string message, ProcessingStepLogLevel level, Dictionary<string, string> properties)
        : this(processingStepId, message, level)
    {
        Properties = properties;
    }
}
