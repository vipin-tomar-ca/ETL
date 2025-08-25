using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Services;

/// <summary>
/// Main interface for ETL engines that can be swapped at runtime
/// </summary>
public interface IETLEngine
{
    /// <summary>
    /// Gets the type of ETL engine
    /// </summary>
    ETLEngineType EngineType { get; }
    
    /// <summary>
    /// Gets the name of the ETL engine
    /// </summary>
    string EngineName { get; }
    
    /// <summary>
    /// Gets the version of the ETL engine
    /// </summary>
    string EngineVersion { get; }
    
    /// <summary>
    /// Validates if the ETL engine can handle the given configuration
    /// </summary>
    Task<ValidationResult> ValidateConfigurationAsync(ETLJobConfiguration configuration, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Executes an ETL job using this engine
    /// </summary>
    Task<ETLExecutionResult> ExecuteAsync(ETLJob job, IProgress<ETLProgress>? progress = null, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Cancels a running ETL job
    /// </summary>
    Task<bool> CancelAsync(Guid jobId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the status of an ETL job
    /// </summary>
    Task<ETLJobStatus> GetStatusAsync(Guid jobId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Tests the connection to the ETL engine
    /// </summary>
    Task<ConnectionTestResult> TestConnectionAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the capabilities of this ETL engine
    /// </summary>
    ETLEngineCapabilities GetCapabilities();
}

/// <summary>
/// Result of ETL execution
/// </summary>
public class ETLExecutionResult
{
    public bool IsSuccess { get; set; }
    public Guid JobId { get; set; }
    public int RecordsProcessed { get; set; }
    public int RecordsFailed { get; set; }
    public TimeSpan Duration { get; set; }
    public string? ErrorMessage { get; set; }
    public Exception? Exception { get; set; }
    public Dictionary<string, object>? Metrics { get; set; }
    
    public static ETLExecutionResult Success(Guid jobId, int recordsProcessed, int recordsFailed, TimeSpan duration) =>
        new() 
        { 
            IsSuccess = true, 
            JobId = jobId, 
            RecordsProcessed = recordsProcessed, 
            RecordsFailed = recordsFailed, 
            Duration = duration 
        };
    
    public static ETLExecutionResult Failure(Guid jobId, string errorMessage, Exception? exception = null) =>
        new() { IsSuccess = false, JobId = jobId, ErrorMessage = errorMessage, Exception = exception };
}

/// <summary>
/// Progress information for ETL execution
/// </summary>
public class ETLProgress
{
    public Guid JobId { get; set; }
    public int RecordsProcessed { get; set; }
    public int RecordsFailed { get; set; }
    public int TotalRecords { get; set; }
    public double PercentageComplete { get; set; }
    public TimeSpan ElapsedTime { get; set; }
    public TimeSpan EstimatedTimeRemaining { get; set; }
    public string CurrentStep { get; set; } = string.Empty;
    public ETLJobStatus Status { get; set; }
}

/// <summary>
/// Capabilities of an ETL engine
/// </summary>
public class ETLEngineCapabilities
{
    public bool SupportsRealTimeProcessing { get; set; }
    public bool SupportsBatchProcessing { get; set; }
    public bool SupportsStreaming { get; set; }
    public bool SupportsParallelProcessing { get; set; }
    public bool SupportsDistributedProcessing { get; set; }
    public bool SupportsIncrementalProcessing { get; set; }
    public bool SupportsDataQualityChecks { get; set; }
    public bool SupportsDataLineage { get; set; }
    public bool SupportsMonitoring { get; set; }
    public bool SupportsScheduling { get; set; }
    public List<string> SupportedDataSources { get; set; } = new();
    public List<string> SupportedDataTargets { get; set; } = new();
    public List<string> SupportedTransformations { get; set; } = new();
    public long MaxDataSize { get; set; } // in bytes
    public int MaxConcurrentJobs { get; set; }
    public TimeSpan MaxJobDuration { get; set; }
}
