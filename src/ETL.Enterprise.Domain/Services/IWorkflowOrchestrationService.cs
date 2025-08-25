using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Services;

/// <summary>
/// Service for orchestrating the multi-tenant ETL workflow
/// </summary>
public interface IWorkflowOrchestrationService
{
    /// <summary>
    /// Starts the complete workflow for a tenant
    /// </summary>
    Task<WorkflowOrchestrationResult> StartWorkflowAsync(
        Guid tenantId,
        ProcessingJobType jobType,
        ProcessingJobConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Executes a specific step in the workflow
    /// </summary>
    Task<WorkflowStepResult> ExecuteStepAsync(
        Guid processingJobId,
        ProcessingStepType stepType,
        ProcessingStepConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Monitors workflow progress
    /// </summary>
    Task<WorkflowProgress> GetWorkflowProgressAsync(
        Guid processingJobId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Triggers child tasks based on completed ingestion jobs
    /// </summary>
    Task<WorkflowOrchestrationResult> TriggerChildTasksAsync(
        Guid tenantId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Polls base database for completed ingestion jobs
    /// </summary>
    Task<List<ProcessingJob>> PollCompletedJobsAsync(
        Guid tenantId,
        ProcessingJobStatus status,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Cancels a workflow
    /// </summary>
    Task<bool> CancelWorkflowAsync(
        Guid processingJobId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Pauses a workflow
    /// </summary>
    Task<bool> PauseWorkflowAsync(
        Guid processingJobId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Resumes a workflow
    /// </summary>
    Task<bool> ResumeWorkflowAsync(
        Guid processingJobId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of workflow orchestration operation
/// </summary>
public class WorkflowOrchestrationResult
{
    public bool IsSuccess { get; set; }
    public ProcessingJob? ProcessingJob { get; set; }
    public string? ErrorMessage { get; set; }
    public List<string> Warnings { get; set; } = new();
    public Dictionary<string, object>? Metadata { get; set; }
    
    public static WorkflowOrchestrationResult Success(ProcessingJob processingJob) =>
        new() { IsSuccess = true, ProcessingJob = processingJob };
    
    public static WorkflowOrchestrationResult Failure(string errorMessage) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage };
}

/// <summary>
/// Result of workflow step execution
/// </summary>
public class WorkflowStepResult
{
    public bool IsSuccess { get; set; }
    public ProcessingJobStep? Step { get; set; }
    public string? ErrorMessage { get; set; }
    public int RecordsProcessed { get; set; }
    public int RecordsFailed { get; set; }
    public TimeSpan Duration { get; set; }
    public Dictionary<string, object>? Output { get; set; }
    
    public static WorkflowStepResult Success(ProcessingJobStep step, int recordsProcessed, int recordsFailed, TimeSpan duration) =>
        new() 
        { 
            IsSuccess = true, 
            Step = step, 
            RecordsProcessed = recordsProcessed, 
            RecordsFailed = recordsFailed, 
            Duration = duration 
        };
    
    public static WorkflowStepResult Failure(string errorMessage) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage };
}

/// <summary>
/// Progress information for a workflow
/// </summary>
public class WorkflowProgress
{
    public Guid ProcessingJobId { get; set; }
    public ProcessingJobStatus Status { get; set; }
    public int TotalSteps { get; set; }
    public int CompletedSteps { get; set; }
    public int FailedSteps { get; set; }
    public int PendingSteps { get; set; }
    public double ProgressPercentage { get; set; }
    public TimeSpan ElapsedTime { get; set; }
    public TimeSpan EstimatedTimeRemaining { get; set; }
    public string CurrentStep { get; set; } = string.Empty;
    public List<ProcessingJobStep> Steps { get; set; } = new();
    public int TotalRecordsProcessed { get; set; }
    public int TotalRecordsFailed { get; set; }
}
