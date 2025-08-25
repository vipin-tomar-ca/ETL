using ETL.Enterprise.Domain.Entities;

namespace ETL.Enterprise.Domain.Services;

/// <summary>
/// Main ETL service interface for orchestrating ETL operations
/// </summary>
public interface IETLService
{
    /// <summary>
    /// Creates and starts a new ETL job
    /// </summary>
    Task<ETLJob> CreateAndStartJobAsync(
        string name, 
        string description, 
        ETLJobConfiguration configuration, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Executes an existing ETL job
    /// </summary>
    Task<ETLJob> ExecuteJobAsync(Guid jobId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Cancels a running ETL job
    /// </summary>
    Task<ETLJob> CancelJobAsync(Guid jobId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Pauses a running ETL job
    /// </summary>
    Task<ETLJob> PauseJobAsync(Guid jobId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Resumes a paused ETL job
    /// </summary>
    Task<ETLJob> ResumeJobAsync(Guid jobId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the status of an ETL job
    /// </summary>
    Task<ETLJob> GetJobStatusAsync(Guid jobId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Validates an ETL job configuration
    /// </summary>
    Task<ValidationResult> ValidateConfigurationAsync(ETLJobConfiguration configuration, CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of configuration validation
/// </summary>
public class ValidationResult
{
    public bool IsValid { get; set; }
    public List<string> Errors { get; set; } = new();
    public List<string> Warnings { get; set; } = new();
    
    public static ValidationResult Success() => new() { IsValid = true };
    
    public static ValidationResult Failure(params string[] errors) => new() 
    { 
        IsValid = false, 
        Errors = errors.ToList() 
    };
}
